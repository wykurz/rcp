//! Fixed bitcode payloads for the current remote compatibility revision.
//! See fixtures/README.md for coverage, limits, and the update procedure.

use remote::protocol::*;

enum Direction {
    Encode,
    Decode,
}

struct Fixtures {
    remaining: std::collections::BTreeMap<&'static str, &'static str>,
    direction: Direction,
}

impl Fixtures {
    fn new(direction: Direction) -> Self {
        let text = match remote::version::WIRE_REVISION {
            14 => include_str!("fixtures/wire-v14.txt"),
            revision => panic!(
                "wire revision {revision} needs reviewed fixtures; see remote/tests/fixtures/README.md"
            ),
        };
        let mut remaining = std::collections::BTreeMap::new();
        for line in text.lines() {
            let (name, bytes) = line.split_once(' ').expect("fixture must be NAME HEX");
            assert!(
                remaining.insert(name, bytes).is_none(),
                "duplicate fixture {name}"
            );
        }
        Self {
            remaining,
            direction,
        }
    }

    fn check<T: serde::Serialize + serde::de::DeserializeOwned>(&mut self, name: &str, value: T) {
        let golden = self
            .remaining
            .remove(name)
            .unwrap_or_else(|| panic!("missing fixture {name}"));
        let bytes = hex::decode(golden).expect("invalid fixture hex");
        match self.direction {
            Direction::Encode => assert_eq!(
                hex::encode(bitcode::serialize(&value).unwrap()),
                golden,
                "{name} changed on the wire; review compatibility before updating fixtures"
            ),
            Direction::Decode => {
                let decoded: T = bitcode::deserialize(&bytes)
                    .unwrap_or_else(|error| panic!("{name} no longer decodes: {error:#}"));
                // compare named values, not re-encoded bytes: a changed decoder can still re-encode identically
                assert_eq!(
                    serde_json::to_value(decoded).unwrap(),
                    serde_json::to_value(value).unwrap(),
                    "{name} decoded to different fields"
                );
            }
        }
    }

    fn finish(self) {
        assert!(
            self.remaining.is_empty(),
            "unexercised fixtures: {:?}",
            self.remaining.keys()
        );
    }
}

fn metadata(acls: WireAcls) -> Metadata {
    Metadata {
        mode: 0o105640,
        uid: 1001,
        gid: 2002,
        atime: -123456789,
        mtime: 1700000001,
        atime_nsec: 123456789,
        mtime_nsec: 987654321,
        acls,
    }
}

fn summary() -> common::copy::Summary {
    common::copy::Summary {
        bytes_copied: 0x123456789,
        files_copied: 2,
        symlinks_created: 3,
        directories_created: 4,
        files_unchanged: 5,
        symlinks_unchanged: 6,
        directories_unchanged: 7,
        files_skipped: 8,
        symlinks_skipped: 9,
        directories_skipped: 10,
        specials_skipped: 11,
        rm_summary: common::rm::Summary {
            bytes_removed: 0x23456789a,
            files_removed: 12,
            symlinks_removed: 13,
            directories_removed: 14,
            files_skipped: 15,
            symlinks_skipped: 16,
            directories_skipped: 17,
        },
    }
}

fn runtime_stats() -> RuntimeStats {
    RuntimeStats {
        cpu_time_user_ms: 257,
        cpu_time_kernel_ms: 65538,
        peak_rss_bytes: 0x3456789ab,
        voluntary_context_switches: 1027,
        involuntary_context_switches: 2053,
        minor_page_faults: 4099,
        major_page_faults: 8209,
    }
}

fn check_messages(direction: Direction) {
    let mut fixtures = Fixtures::new(direction);
    let mut filter = common::filter::FilterSettings::new();
    filter.add_include("**/*.rs").unwrap();
    filter.add_include("docs/**").unwrap();
    filter.add_exclude("target/**").unwrap();
    for (name, dry_run, capture) in [
        (
            "master-source-brief",
            Some(common::config::DryRunMode::Brief),
            ExtendedMetadataCapture {
                file_acl: true,
                dir_acl: false,
                root_acl_notice: false,
            },
        ),
        (
            "master-source-all",
            Some(common::config::DryRunMode::All),
            ExtendedMetadataCapture {
                file_acl: false,
                dir_acl: true,
                root_acl_notice: false,
            },
        ),
        (
            "master-source-explain",
            Some(common::config::DryRunMode::Explain),
            ExtendedMetadataCapture {
                file_acl: false,
                dir_acl: false,
                root_acl_notice: true,
            },
        ),
    ] {
        fixtures.check(
            name,
            MasterToRcpd::Hello(Box::new(MasterHello::Source {
                src: "/src/α".into(),
                dst: "/dst/b".into(),
                dest_cert_fingerprint: Some(std::array::from_fn(|i| i as u8)),
                filter: Some(filter.clone()),
                dry_run,
                capture,
                accept_timeout_sec: 30,
            })),
        );
    }
    fixtures.check(
        "master-source-none",
        MasterToRcpd::Hello(Box::new(MasterHello::Source {
            src: "/source".into(),
            dst: "/destination".into(),
            dest_cert_fingerprint: None,
            filter: None,
            dry_run: None,
            capture: ExtendedMetadataCapture {
                file_acl: false,
                dir_acl: false,
                root_acl_notice: false,
            },
            accept_timeout_sec: 15,
        })),
    );
    let preserve = common::preserve::Settings {
        file: common::preserve::FileSettings {
            user_and_time: common::preserve::UserAndTimeSettings {
                uid: true,
                gid: false,
                time: false,
            },
            mode_mask: 0o640,
            acl: true,
        },
        dir: common::preserve::DirSettings {
            user_and_time: common::preserve::UserAndTimeSettings {
                uid: false,
                gid: true,
                time: false,
            },
            mode_mask: 0o2750,
            acl: false,
        },
        symlink: common::preserve::SymlinkSettings {
            user_and_time: common::preserve::UserAndTimeSettings {
                uid: false,
                gid: false,
                time: true,
            },
        },
    };
    for (name, fingerprint, dry_run, data_addrs, transport) in [
        (
            "master-destination-tls",
            Some(std::array::from_fn(|i| 255 - i as u8)),
            false,
            vec!["[2001:db8::1234]:3456".parse().unwrap()],
            DestinationTransport::Direct,
        ),
        (
            "master-destination-master-ssh",
            Some(std::array::from_fn(|i| i as u8)),
            false,
            vec![
                "127.0.0.1:40001".parse().unwrap(),
                "127.0.0.1:40002".parse().unwrap(),
            ],
            DestinationTransport::MasterSshTunnel,
        ),
        (
            "master-destination-preview",
            None,
            true,
            vec![],
            DestinationTransport::AutoMasterSsh {
                probe_timeout_sec: 3,
            },
        ),
        (
            "master-destination-direct-ssh",
            Some(std::array::from_fn(|i| 128 ^ (i as u8))),
            false,
            vec![
                "127.0.0.1:41001".parse().unwrap(),
                "127.0.0.1:41002".parse().unwrap(),
            ],
            DestinationTransport::DirectSsh {
                source_ssh: SshTarget {
                    user: Some("deploy".into()),
                    host: "10.20.30.40".into(),
                    port: Some(2222),
                },
                tunnel_target_ip: std::net::IpAddr::V4(std::net::Ipv4Addr::LOCALHOST),
                ssh_tunnels: 4,
                dest_root: "/dst/tree".into(),
                source_peer: PeerIdentity {
                    hostname: "node01".into(),
                    boot_id: Some("11111111-1111-1111-1111-111111111111".into()),
                    net_ns: Some("net:[4026531992]".into()),
                    ssh_server_port: Some(22),
                },
                source_node_endpoint: Some("10.20.30.40:2222".parse().unwrap()),
            },
        ),
        (
            "master-destination-auto-direct-ssh",
            None,
            false,
            vec!["198.51.100.9:5678".parse().unwrap()],
            DestinationTransport::AutoDirectSsh {
                probe_timeout_sec: 5,
                source_ssh: SshTarget {
                    user: None,
                    host: "source.internal".into(),
                    port: None,
                },
                tunnel_target_ip: std::net::IpAddr::V6(std::net::Ipv6Addr::LOCALHOST),
                ssh_tunnels: 2,
                dest_root: "/tmp/dst".into(),
                source_peer: PeerIdentity {
                    hostname: "node02".into(),
                    boot_id: None,
                    net_ns: None,
                    ssh_server_port: None,
                },
                source_node_endpoint: None,
            },
        ),
    ] {
        fixtures.check(
            name,
            MasterToRcpd::Hello(Box::new(MasterHello::Destination {
                source_control_addr: "192.0.2.7:2345".parse().unwrap(),
                source_data_addrs: data_addrs,
                server_name: "source.example".into(),
                preserve,
                source_cert_fingerprint: fingerprint,
                dry_run,
                transport: Box::new(transport),
            })),
        );
    }
    fixtures.check("master-heartbeat", MasterToRcpd::Heartbeat);
    fixtures.check(
        "master-ssh-fallback-endpoints",
        MasterToRcpd::SshFallbackEndpoints {
            control_addr: "127.0.0.1:42000".parse().unwrap(),
            data_addrs: vec![
                "127.0.0.1:42001".parse().unwrap(),
                "127.0.0.1:42002".parse().unwrap(),
            ],
        },
    );
    fixtures.check(
        "source-master",
        RcpdToMaster::SourceHello(SourceMasterHello {
            control_addr: "[2001:db8::5678]:4567".parse().unwrap(),
            data_addr: "198.51.100.9:5678".parse().unwrap(),
            tunnel_target_ip: std::net::IpAddr::V4(std::net::Ipv4Addr::new(198, 51, 100, 9)),
            server_name: "data.example".into(),
            peer_identity: PeerIdentity {
                hostname: "node01".into(),
                boot_id: Some("11111111-1111-1111-1111-111111111111".into()),
                net_ns: Some("net:[4026531992]".into()),
                ssh_server_port: Some(22),
            },
            ssh_node_endpoint: Some("198.51.100.9:22".parse().unwrap()),
        }),
    );
    fixtures.check(
        "direct-connect-failed",
        RcpdToMaster::DirectConnectFailed {
            reason: "connection to 198.51.100.9:4567 timed out after 3s".into(),
        },
    );
    fixtures.check(
        "result-success",
        RcpdToMaster::Result(RcpdResult::Success {
            message: "copied α".into(),
            summary: summary(),
            runtime_stats: runtime_stats(),
        }),
    );
    fixtures.check(
        "result-failure",
        RcpdToMaster::Result(RcpdResult::Failure {
            error: "copy failed: permission denied".into(),
            summary: summary(),
            runtime_stats: runtime_stats(),
            peer_connected: true,
        }),
    );
    fixtures.check(
        "directory-limits",
        DirectoryLimits {
            normal: std::num::NonZeroUsize::new(257).unwrap(),
            reserve: std::num::NonZeroUsize::new(1025).unwrap(),
        },
    );
    for (name, admission, is_root, keep_if_empty, acls) in [
        (
            "directory-begin-normal",
            DirectoryClass::Normal,
            true,
            false,
            WireAcls::Unknown,
        ),
        (
            "directory-begin-reserve",
            DirectoryClass::Reserve,
            false,
            true,
            WireAcls::Captured {
                access: Some(vec![2, 0, 1, 7, 255]),
                default: Some(vec![2, 0, 4, 5]),
            },
        ),
    ] {
        fixtures.check(
            name,
            SourceMessage::DirectoryBegin {
                src: "/src/d".into(),
                dst: "/dst/e".into(),
                metadata: metadata(acls),
                is_root,
                keep_if_empty,
                admission,
            },
        );
    }
    fixtures.check(
        "directory-end",
        SourceMessage::DirectoryEnd {
            src: "/src/d".into(),
            dst: "/dst/e".into(),
            entry_count: 65537,
        },
    );
    fixtures.check(
        "symlink",
        SourceMessage::Symlink {
            src: "/src/link".into(),
            dst: "/dst/link".into(),
            target: "../α".into(),
            metadata: metadata(WireAcls::Unknown),
            is_root: true,
        },
    );
    fixtures.check(
        "discovery-root",
        SourceMessage::DiscoveryComplete {
            has_root_item: true,
        },
    );
    fixtures.check(
        "discovery-empty",
        SourceMessage::DiscoveryComplete {
            has_root_item: false,
        },
    );
    fixtures.check(
        "file-skipped",
        SourceMessage::FileSkipped {
            src: "/src/f".into(),
            dst: "/dst/g".into(),
        },
    );
    fixtures.check(
        "file-unchanged",
        SourceMessage::FileUnchanged {
            src: "/src/f".into(),
            dst: "/dst/g".into(),
        },
    );
    fixtures.check("dry-run-ready", SourceMessage::DryRunReady);
    fixtures.check(
        "manifest",
        DestinationMessage::DirectoryManifestChunk {
            dst: "/dst/e".into(),
            entries: vec![
                ExistingEntry {
                    name: "α.rs".into(),
                    is_file: true,
                    metadata: metadata(WireAcls::Unknown),
                    size: 0x456789abc,
                },
                ExistingEntry {
                    name: "subdir".into(),
                    is_file: false,
                    metadata: metadata(WireAcls::Unknown),
                    size: 0,
                },
            ],
        },
    );
    fixtures.check(
        "directory-ready",
        DestinationMessage::DirectoryReady {
            src: "/src/d".into(),
            dst: "/dst/e".into(),
        },
    );
    fixtures.check(
        "directory-skipped",
        DestinationMessage::DirectorySkipped {
            src: "/src/d".into(),
            dst: "/dst/e".into(),
        },
    );
    fixtures.check(
        "directory-released",
        DestinationMessage::DirectoryReleased {
            src: "/src/d".into(),
            dst: "/dst/e".into(),
        },
    );
    fixtures.check("destination-done", DestinationMessage::DestinationDone);
    for (name, acls, is_root) in [
        (
            "file-acl",
            WireAcls::Captured {
                access: Some(vec![2, 0, 1, 6, 255]),
                default: None,
            },
            false,
        ),
        (
            "file-no-acl",
            WireAcls::Captured {
                access: None,
                default: None,
            },
            true,
        ),
    ] {
        fixtures.check(
            name,
            File {
                src: "/src/file".into(),
                dst: "/dst/file".into(),
                size: 0x56789abcd,
                metadata: metadata(acls),
                is_root,
            },
        );
    }
    let timestamp = std::time::UNIX_EPOCH + std::time::Duration::new(1700000123, 456789123);
    fixtures.check(
        "tracing-log",
        RcpdToMaster::Tracing(common::remote_tracing::TracingMessage::Log {
            timestamp,
            level: "WARN".into(),
            target: "rcp::wire".into(),
            message: "example α".into(),
        }),
    );
    fixtures.check(
        "tracing-progress",
        RcpdToMaster::Tracing(common::remote_tracing::TracingMessage::Progress(
            common::progress::SerializableProgress {
                ops_started: 101,
                ops_finished: 99,
                bytes_copied: 0x6789abcde,
                hard_links_created: 2,
                files_copied: 3,
                symlinks_created: 4,
                directories_created: 5,
                files_unchanged: 6,
                symlinks_unchanged: 7,
                directories_unchanged: 8,
                hard_links_unchanged: 9,
                files_removed: 10,
                symlinks_removed: 11,
                directories_removed: 12,
                bytes_removed: 0x789abcdef,
                files_skipped: 13,
                symlinks_skipped: 14,
                directories_skipped: 15,
                specials_skipped: 16,
                current_time: timestamp,
            },
        )),
    );
    fixtures.finish();
}

#[test]
fn encodes_revision_fixtures() {
    check_messages(Direction::Encode);
}

#[test]
fn decodes_revision_fixtures() {
    check_messages(Direction::Decode);
}
