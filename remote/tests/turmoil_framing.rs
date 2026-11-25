//! Exercise the real stream codecs over simulated TCP, without daemon or filesystem setup.

use std::sync::Arc;
use std::time::Duration;

use remote::protocol::{DestinationMessage, File, Metadata, WireAcls};
use remote::streams::{RecvStream, SendStream};
use tokio::io::AsyncWriteExt;
use tokio::sync::Notify;
use turmoil::net::{TcpListener, TcpStream};

fn file(name: &str, size: u64) -> File {
    File {
        src: format!("/src/{name}").into(),
        dst: format!("/dst/{name}").into(),
        size,
        metadata: Metadata {
            mode: 0o644,
            uid: 0,
            gid: 0,
            atime: 0,
            mtime: 0,
            atime_nsec: 0,
            mtime_nsec: 0,
            acls: WireAcls::Unknown,
        },
        is_root: false,
    }
}

#[test]
fn held_payload_resumes_at_next_file_boundary() -> turmoil::Result {
    let mut sim = turmoil::Builder::new()
        .simulation_duration(Duration::from_secs(2))
        .build();

    // Order fault injection explicitly: the source queues the tail only after the link is held.
    let send_tail = Arc::new(Notify::new());
    let source_gate = Arc::clone(&send_tail);
    sim.client("source", async move {
        let listener = TcpListener::bind(("0.0.0.0", 1234)).await?;
        let (mut data, _) = listener.accept().await?;
        SendStream::new(&mut data)
            .send_batch_message(&file("first", 4))
            .await?;
        data.write_all(b"ab").await?;
        source_gate.notified().await;
        data.write_all(b"cd").await?;
        SendStream::new(&mut data)
            .send_batch_message(&file("second", 1))
            .await?;
        data.write_all(b"e").await?;
        data.shutdown().await?;
        Ok(())
    });

    sim.client("destination", async move {
        let mut data = RecvStream::new(TcpStream::connect(("source", 1234)).await?);
        let first = data.recv_object::<File>().await?.expect("first header");
        assert_eq!(first.dst, std::path::Path::new("/dst/first"));
        let mut output = Vec::new();
        {
            let mut body = std::pin::pin!(data.copy_exact_to_buffered(&mut output, first.size, 2));
            turmoil::hold("source", "destination");
            send_tail.notify_one();
            assert!(
                tokio::time::timeout(Duration::from_millis(15), body.as_mut())
                    .await
                    .is_err(),
                "the file must not complete while its tail is held"
            );
            turmoil::release("source", "destination");
            assert_eq!(body.await?, first.size);
        }
        assert_eq!(output, b"abcd");

        let second = data.recv_object::<File>().await?.expect("second header");
        assert_eq!(second.dst, std::path::Path::new("/dst/second"));
        output.clear();
        assert_eq!(
            data.copy_exact_to_buffered(&mut output, second.size, 2)
                .await?,
            second.size
        );
        assert_eq!(output, b"e");
        assert!(data.recv_object::<File>().await?.is_none());
        Ok(())
    });

    sim.run()
}

#[test]
fn truncated_data_does_not_close_the_control_stream() -> turmoil::Result {
    let mut sim = turmoil::Builder::new()
        .simulation_duration(Duration::from_secs(2))
        .build();

    sim.client("source", async {
        let data_listener = TcpListener::bind(("0.0.0.0", 1234)).await?;
        let control_listener = TcpListener::bind(("0.0.0.0", 1235)).await?;
        let (mut data, _) = data_listener.accept().await?;
        let (control, _) = control_listener.accept().await?;
        SendStream::new(&mut data)
            .send_batch_message(&file("first", 4))
            .await?;
        data.write_all(b"ab").await?;
        data.shutdown().await?;

        let mut control = RecvStream::new(control);
        assert!(matches!(
            control.recv_object::<DestinationMessage>().await?,
            Some(DestinationMessage::DirectoryReady { dst, .. })
                if dst == std::path::Path::new("/dst/other")
        ));
        Ok(())
    });

    sim.client("destination", async {
        let mut data = RecvStream::new(TcpStream::connect(("source", 1234)).await?);
        let mut control = SendStream::new(TcpStream::connect(("source", 1235)).await?);
        let header = data.recv_object::<File>().await?.expect("file header");
        let mut output = Vec::new();
        let error = data
            .copy_exact_to_buffered(&mut output, header.size, 2)
            .await
            .expect_err("clean TCP EOF halfway through the declared body must be rejected");
        assert!(format!("{error:#}").contains("unexpected EOF: expected 4 bytes, got 2"));
        assert_eq!(output, b"ab");

        // Only the data socket was closed; prove this was not an accidental link-wide fault.
        control
            .send_control_message(&DestinationMessage::DirectoryReady {
                src: "/src/other".into(),
                dst: "/dst/other".into(),
            })
            .await?;
        Ok(())
    });

    sim.run()
}
