//! Which TCP socket buffer sizes a remote copy asks the kernel for.
//!
//! Asking is not harmless. The kernel cuts a request to a limit of its own without saying so, and
//! then never changes that buffer again, while a buffer nobody asked for is grown by the kernel as
//! the connection needs, up to a separate and often much larger limit
//! ([`common::host::TcpBufferLimits`]). On a host with stock limits of a kernel before 6.18, a
//! request for 16 MiB is granted 208 KiB, where the kernel alone would have reached several MiB.
//!
//! So each end of a data connection asks only where asking yields more than the kernel would reach
//! by itself: the source for its send buffer, the destination for its receive buffer. Each decides
//! from the limits of its own host.
//!
//! The two requests are different types because they obey different rules. A send buffer can be
//! asked for on a connection that already exists. A receive buffer has to be asked for before the
//! socket connects: on older kernels without the receive-window clamp fix, a later request does not
//! update the clamp, so the advertised window can remain limited by the initial buffer sizing.
//! Only the function that configures a socket before it connects accepts a [`ReceiveBufferRequest`]. That
//! function also takes the kernel's "fixed" mark off the buffer again before the socket connects:
//! the size stays, but a buffer still marked at that moment would narrow the connection's window
//! scale to half the buffer.
//!
//! Outside tests, a request for a size only comes out of [`SendBufferRequest::for_this_host`] and
//! [`ReceiveBufferRequest::for_this_host`], so a size that the limits of this host do not call for
//! cannot reach a socket.
//!
//! Each end logs its decision once at info level. The destination also warns, once, where its
//! kernel does not grow receive buffers and leaves it with less than the profile's size: nothing
//! this module can ask for gets such a host past its limits, only its administrator can.

use common::host::TcpBufferLimits;

/// A send buffer size to ask the kernel for, or nothing. The end that sends file data, the source
/// `rcpd`, asks for one.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct SendBufferRequest(Option<usize>);

/// A receive buffer size to ask the kernel for, or nothing. The end that receives file data, the
/// destination `rcpd`, asks for one, and only before it connects.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct ReceiveBufferRequest(Option<usize>);

impl SendBufferRequest {
    const DIRECTION: Direction = Direction::Send;

    /// Ask for nothing.
    #[cfg(test)]
    pub(crate) const NONE: Self = Self(None);

    /// What to ask for when `size` is wanted on a host with these limits: `size` if that yields
    /// more than the kernel's own growth would, nothing otherwise or when the limits are unknown.
    pub(crate) fn plan(size: usize, limits: Option<&TcpBufferLimits>) -> Self {
        Self(Self::DIRECTION.plan(size, limits))
    }

    /// What the source asks for on its data connections under `profile`, on the host it runs
    /// on. The decision and the figures behind it are logged once.
    pub(crate) fn for_this_host(profile: crate::NetworkProfile) -> Self {
        static ANNOUNCED: std::sync::Once = std::sync::Once::new();
        let (size, limits) = this_host(profile);
        let request = Self::plan(size, limits.ok());
        ANNOUNCED
            .call_once(|| tracing::info!("{}", Self::DIRECTION.describe(request.0, size, limits)));
        request
    }

    /// The size to ask for, if any.
    pub(crate) fn size(self) -> Option<usize> {
        self.0
    }

    /// A request for `size`, whatever any host's limits say.
    #[cfg(test)]
    pub(crate) fn of(size: usize) -> Self {
        Self(Some(size))
    }
}

impl ReceiveBufferRequest {
    const DIRECTION: Direction = Direction::Receive;

    /// Ask for nothing.
    #[cfg(test)]
    pub(crate) const NONE: Self = Self(None);

    /// What to ask for when `size` is wanted on a host with these limits: `size` if that yields
    /// more than the kernel's own growth would, nothing otherwise or when the limits are unknown.
    pub(crate) fn plan(size: usize, limits: Option<&TcpBufferLimits>) -> Self {
        Self(Self::DIRECTION.plan(size, limits))
    }

    /// What the destination asks for on its data connections under `profile`, on the host it
    /// runs on. The decision and the figures behind it are logged once, with a warning where the
    /// host starves these buffers (see `starved_receive_warning`).
    pub(crate) fn for_this_host(profile: crate::NetworkProfile) -> Self {
        static ANNOUNCED: std::sync::Once = std::sync::Once::new();
        let (size, limits) = this_host(profile);
        let request = Self::plan(size, limits.ok());
        ANNOUNCED.call_once(|| announce_receive(request.0, size, limits));
        request
    }

    /// The size to ask for, if any.
    pub(crate) fn size(self) -> Option<usize> {
        self.0
    }

    /// A request for `size`, whatever any host's limits say.
    #[cfg(test)]
    pub(crate) fn of(size: usize) -> Self {
        Self(Some(size))
    }
}

/// Log what the destination decided for its receive buffers, and warn where its host starves them.
///
/// The decision is detail, shown with `-v`. The warning is a notice: the master prints it at its
/// default verbosity, because the user it is for does not know there is anything to ask about.
fn announce_receive(requested: Option<usize>, size: usize, limits: Result<&TcpBufferLimits, &str>) {
    tracing::info!("{}", Direction::Receive.describe(requested, size, limits));
    if let Some(warning) = limits
        .ok()
        .and_then(|limits| starved_receive_warning(size, limits))
    {
        tracing::warn!(target: common::NOTICE_TARGET, "{warning}");
    }
}

/// The warning for a host whose kernel does not grow receive buffers and leaves a data connection
/// with one smaller than `size`, the size its network profile asks for. `None` for any other host.
///
/// What a data connection ends up with on such a host is the larger of what a request is granted
/// and what every buffer keeps. A host that keeps its buffers at the profile's size or more is
/// left alone: that is a deliberate setting, not a starved one. So is a host whose buffers grow,
/// however little: how far they get is in the decision line.
fn starved_receive_warning(size: usize, limits: &TcpBufferLimits) -> Option<String> {
    let (granted, kept) = Direction::Receive.figures(size, limits);
    let buffer = granted.max(kept);
    let wanted = u64::try_from(size).unwrap_or(u64::MAX);
    (!limits.receive_buffers_grow && buffer < wanted).then(|| {
        format!(
            "TCP receive buffers of data connections do not grow on this host \
             (net.ipv4.tcp_moderate_rcvbuf is 0) and stay at {}, where this network profile asks \
             for {}: copies over links with latency will be slow. Raise net.core.rmem_max, raise \
             the middle value of net.ipv4.tcp_rmem, or set net.ipv4.tcp_moderate_rcvbuf to 1",
            bytesize::ByteSize(buffer),
            bytesize::ByteSize(wanted)
        )
    })
}

/// The size `profile` wants, and the limits of the host this process runs on.
fn this_host(
    profile: crate::NetworkProfile,
) -> (usize, Result<&'static TcpBufferLimits, &'static str>) {
    (
        profile.socket_buffer_size(),
        common::host::current().tcp_buffers(),
    )
}

/// Which of a socket's two buffers a decision is about.
#[derive(Clone, Copy)]
enum Direction {
    Send,
    Receive,
}

impl Direction {
    /// The two figures a decision compares, both as the kernel accounts them: what a request for
    /// `size` would be granted, and how far the kernel grows the buffer by itself.
    fn figures(self, size: usize, limits: &TcpBufferLimits) -> (u64, u64) {
        match self {
            Self::Send => (limits.send_buffer_for_request(size), limits.send_growth),
            Self::Receive => (
                limits.receive_buffer_for_request(size),
                limits.receive_growth,
            ),
        }
    }

    /// The size to request, if any.
    ///
    /// A buffer is requested only if the request yields more than the kernel's own growth would;
    /// on a tie the kernel keeps it, because a buffer it sizes itself adapts and holds no more
    /// memory than the connection uses. Without known limits nothing is requested.
    fn plan(self, size: usize, limits: Option<&TcpBufferLimits>) -> Option<usize> {
        let (granted, growth) = self.figures(size, limits?);
        (granted > growth).then_some(size)
    }

    /// A decision in words, with the figures it rests on.
    fn describe(
        self,
        requested: Option<usize>,
        size: usize,
        limits: Result<&TcpBufferLimits, &str>,
    ) -> String {
        let direction = match self {
            Self::Send => "send",
            Self::Receive => "receive",
        };
        let wanted = bytesize::ByteSize(size as u64);
        let limits = match limits {
            Ok(limits) => limits,
            Err(reason) => {
                return format!(
                    "TCP {direction} buffers of data connections: left to the kernel, whose \
                     limits could not be found out: {reason}"
                );
            }
        };
        let (granted, growth) = self.figures(size, limits);
        let (granted, growth) = (bytesize::ByteSize(granted), bytesize::ByteSize(growth));
        // only receive buffers can be kept from growing (`net.ipv4.tcp_moderate_rcvbuf`)
        let own = match self {
            Self::Receive if !limits.receive_buffers_grow => "keeps them at",
            Self::Send | Self::Receive => "grows them to",
        };
        if requested.is_some() {
            format!(
                "TCP {direction} buffers of data connections: requested at {wanted}, which the \
                 kernel grants as {granted}, more than the {growth} it {own} by itself"
            )
        } else {
            format!(
                "TCP {direction} buffers of data connections: left to the kernel, which {own} \
                 {growth}; a request for {wanted} would be held at {granted}"
            )
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const MIB: u64 = 1 << 20;
    const DATACENTER: usize = 16 << 20;
    const INTERNET: usize = 2 << 20;

    /// The six host configurations measured on kernels 6.1, 6.6 and 6.12 (2026-11): for each, the
    /// requests below are the ones that left the most data in flight per round trip.
    fn hosts() -> [(&'static str, TcpBufferLimits); 6] {
        let limits = |request: u64, send_growth: u64, receive_growth: u64| TcpBufferLimits {
            send_request: request,
            receive_request: request,
            send_growth,
            receive_growth,
            receive_buffers_grow: true,
        };
        [
            (
                "stock, kernel before 6.18",
                limits(212_992, 4 * MIB, 6 * MIB),
            ),
            (
                "request limit raised to 16 MiB",
                limits(16 * MIB, 4 * MIB, 6 * MIB),
            ),
            (
                "everything raised to 32 MiB",
                limits(32 * MIB, 32 * MIB, 32 * MIB),
            ),
            (
                "growth raised to 16 MiB, request limit stock",
                limits(212_992, 16 * MIB, 16 * MIB),
            ),
            // with tcp_moderate_rcvbuf off a receive buffer stays at its starting 128 KiB
            (
                "stock, receive growth off",
                TcpBufferLimits {
                    receive_buffers_grow: false,
                    ..limits(212_992, 4 * MIB, 131_072)
                },
            ),
            (
                "stock, kernel 6.18 and later",
                limits(4 * MIB, 4 * MIB, 32 * MIB),
            ),
        ]
    }

    /// Both ends' decisions for one host.
    fn plan(
        size: usize,
        limits: Option<&TcpBufferLimits>,
    ) -> (SendBufferRequest, ReceiveBufferRequest) {
        (
            SendBufferRequest::plan(size, limits),
            ReceiveBufferRequest::plan(size, limits),
        )
    }

    fn requests(
        send: Option<usize>,
        receive: Option<usize>,
    ) -> (SendBufferRequest, ReceiveBufferRequest) {
        (SendBufferRequest(send), ReceiveBufferRequest(receive))
    }

    #[test]
    fn datacenter_buffers_are_requested_only_where_a_request_beats_the_kernel() {
        let expected = [
            // 416 KiB granted against 4 and 6 MiB of growth
            (None, None),
            // 32 MiB granted against 4 and 6 MiB
            (Some(DATACENTER), Some(DATACENTER)),
            // 32 MiB either way: a tie, left to the kernel
            (None, None),
            // 416 KiB granted against 16 MiB
            (None, None),
            // sending: 416 KiB against 4 MiB; receiving: 416 KiB against a buffer stuck at 128 KiB
            (None, Some(DATACENTER)),
            // sending: 8 MiB granted against 4 MiB; receiving: 8 MiB against 32 MiB
            (Some(DATACENTER), None),
        ];
        for ((name, limits), (send, receive)) in hosts().into_iter().zip(expected) {
            assert_eq!(
                plan(DATACENTER, Some(&limits)),
                requests(send, receive),
                "{name}"
            );
        }
    }

    #[test]
    fn internet_buffers_are_left_to_the_kernel_wherever_it_grows_them_as_far() {
        // 2 MiB is granted as 4 MiB at most, which no stock growth limit falls short of
        let expected = [
            (None, None),
            (None, None),
            (None, None),
            (None, None),
            (None, Some(INTERNET)),
            (None, None),
        ];
        for ((name, limits), (send, receive)) in hosts().into_iter().zip(expected) {
            assert_eq!(
                plan(INTERNET, Some(&limits)),
                requests(send, receive),
                "{name}"
            );
        }
    }

    #[test]
    fn each_direction_is_decided_by_its_own_limits() {
        let limits = TcpBufferLimits {
            send_request: 16 * MIB,
            receive_request: 212_992,
            send_growth: 4 * MIB,
            receive_growth: 6 * MIB,
            receive_buffers_grow: true,
        };
        assert_eq!(
            plan(DATACENTER, Some(&limits)),
            requests(Some(DATACENTER), None)
        );
        let limits = TcpBufferLimits {
            send_request: 212_992,
            receive_request: 16 * MIB,
            send_growth: 4 * MIB,
            receive_growth: 6 * MIB,
            receive_buffers_grow: true,
        };
        assert_eq!(
            plan(DATACENTER, Some(&limits)),
            requests(None, Some(DATACENTER))
        );
        // the growth limits are each direction's own as well
        let limits = TcpBufferLimits {
            send_request: 16 * MIB,
            receive_request: 16 * MIB,
            send_growth: 64 * MIB,
            receive_growth: 6 * MIB,
            receive_buffers_grow: true,
        };
        assert_eq!(
            plan(DATACENTER, Some(&limits)),
            requests(None, Some(DATACENTER))
        );
    }

    #[test]
    fn a_request_has_to_beat_the_kernel_by_at_least_a_byte() {
        let limits = |growth: u64| TcpBufferLimits {
            send_request: 16 * MIB,
            receive_request: 16 * MIB,
            send_growth: growth,
            receive_growth: growth,
            receive_buffers_grow: true,
        };
        // a full grant of 16 MiB is accounted as 32 MiB
        assert_eq!(
            plan(DATACENTER, Some(&limits(32 * MIB))),
            requests(None, None)
        );
        assert_eq!(
            plan(DATACENTER, Some(&limits(32 * MIB - 1))),
            requests(Some(DATACENTER), Some(DATACENTER))
        );
    }

    #[test]
    fn nothing_is_requested_when_the_limits_are_unknown_or_nothing_is_wanted() {
        assert_eq!(plan(DATACENTER, None), requests(None, None));
        let generous = TcpBufferLimits {
            send_request: 64 * MIB,
            receive_request: 64 * MIB,
            send_growth: 0,
            receive_growth: 0,
            receive_buffers_grow: true,
        };
        assert_eq!(plan(0, Some(&generous)), requests(None, None));
    }

    #[test]
    fn a_request_carries_the_size_it_asks_for() {
        assert_eq!(SendBufferRequest::NONE.size(), None);
        assert_eq!(ReceiveBufferRequest::NONE.size(), None);
        assert_eq!(SendBufferRequest::of(4096).size(), Some(4096));
        assert_eq!(ReceiveBufferRequest::of(4096).size(), Some(4096));
        let generous = TcpBufferLimits {
            send_request: 64 * MIB,
            receive_request: 64 * MIB,
            send_growth: 0,
            receive_growth: 0,
            receive_buffers_grow: true,
        };
        assert_eq!(
            SendBufferRequest::plan(DATACENTER, Some(&generous)).size(),
            Some(DATACENTER)
        );
        assert_eq!(
            ReceiveBufferRequest::plan(INTERNET, Some(&generous)).size(),
            Some(INTERNET)
        );
    }

    #[test]
    fn each_end_describes_its_own_decision_with_the_figures_behind_it() {
        // stock values of kernel 6.18: a request beats the kernel for sending only
        let limits = TcpBufferLimits {
            send_request: 4 * MIB,
            receive_request: 4 * MIB,
            send_growth: 4 * MIB,
            receive_growth: 32 * MIB,
            receive_buffers_grow: true,
        };
        assert_eq!(
            Direction::Send.describe(Some(DATACENTER), DATACENTER, Ok(&limits)),
            "TCP send buffers of data connections: requested at 16.0 MiB, which the kernel \
             grants as 8.0 MiB, more than the 4.0 MiB it grows them to by itself"
        );
        assert_eq!(
            Direction::Receive.describe(None, DATACENTER, Ok(&limits)),
            "TCP receive buffers of data connections: left to the kernel, which grows them to \
             32.0 MiB; a request for 16.0 MiB would be held at 8.0 MiB"
        );
        assert_eq!(
            Direction::Receive.describe(
                None,
                DATACENTER,
                Err("kernel setting ipv4/tcp_rmem: No such file or directory")
            ),
            "TCP receive buffers of data connections: left to the kernel, whose limits could not \
             be found out: kernel setting ipv4/tcp_rmem: No such file or directory"
        );
    }

    /// A host whose kernel does not grow receive buffers: they keep `fixed` bytes, and a request
    /// is granted `request_limit` at most.
    fn without_receive_growth(request_limit: u64, fixed: u64) -> TcpBufferLimits {
        TcpBufferLimits {
            send_request: request_limit,
            receive_request: request_limit,
            send_growth: 4 * MIB,
            receive_growth: fixed,
            receive_buffers_grow: false,
        }
    }

    #[test]
    fn a_host_that_cannot_grow_small_receive_buffers_is_warned_about() {
        // what a data connection ends up with is the larger of what a request is granted and
        // what every buffer keeps; the warning is for less than the profile's size
        let cases = [
            // the stock request limit grants 416 KiB, more than the 128 KiB buffers start with
            (
                without_receive_growth(212_992, 131_072),
                DATACENTER,
                Some("416.0 KiB"),
            ),
            (
                without_receive_growth(212_992, 131_072),
                INTERNET,
                Some("416.0 KiB"),
            ),
            // buffers fixed at 1 MiB, more than a request would get
            (
                without_receive_growth(212_992, MIB),
                DATACENTER,
                Some("1.0 MiB"),
            ),
            (
                without_receive_growth(212_992, MIB),
                INTERNET,
                Some("1.0 MiB"),
            ),
            // buffers fixed at 2 MiB: enough for the internet profile, not for the datacenter one
            (without_receive_growth(212_992, 2 * MIB), INTERNET, None),
            (
                without_receive_growth(212_992, 2 * MIB),
                DATACENTER,
                Some("2.0 MiB"),
            ),
            // buffers fixed at the profile's size, as an administrator who wants fixed buffers
            // sets them: nothing to warn about
            (without_receive_growth(212_992, 16 * MIB), DATACENTER, None),
            (without_receive_growth(212_992, 16 * MIB), INTERNET, None),
            // small fixed buffers, but a request is granted in full
            (without_receive_growth(16 * MIB, 131_072), DATACENTER, None),
        ];
        for (limits, size, held_at) in cases {
            let warning = starved_receive_warning(size, &limits);
            match held_at {
                Some(held_at) => {
                    let warning = warning.unwrap_or_else(|| panic!("no warning: {limits:?}"));
                    assert!(warning.contains(held_at), "{warning}");
                }
                None => assert_eq!(warning, None, "{limits:?} wanting {size}"),
            }
        }
    }

    #[test]
    fn a_host_that_grows_its_receive_buffers_is_not_warned_about() {
        // however far below the profile's size they stop: that is what the decision line is for
        for (name, limits) in hosts() {
            if limits.receive_buffers_grow {
                for size in [DATACENTER, INTERNET] {
                    assert_eq!(starved_receive_warning(size, &limits), None, "{name}");
                }
            }
        }
        let grows_little = TcpBufferLimits {
            send_request: 212_992,
            receive_request: 212_992,
            send_growth: 4 * MIB,
            receive_growth: MIB,
            receive_buffers_grow: true,
        };
        assert_eq!(starved_receive_warning(DATACENTER, &grows_little), None);
    }

    #[test]
    fn the_warning_names_the_cause_the_sizes_and_the_three_ways_out() {
        assert_eq!(
            starved_receive_warning(DATACENTER, &without_receive_growth(212_992, 131_072))
                .as_deref(),
            Some(
                "TCP receive buffers of data connections do not grow on this host \
                 (net.ipv4.tcp_moderate_rcvbuf is 0) and stay at 416.0 KiB, where this network \
                 profile asks for 16.0 MiB: copies over links with latency will be slow. Raise \
                 net.core.rmem_max, raise the middle value of net.ipv4.tcp_rmem, or set \
                 net.ipv4.tcp_moderate_rcvbuf to 1"
            )
        );
    }

    #[test]
    fn a_decision_says_keeps_where_receive_buffers_do_not_grow() {
        let limits = without_receive_growth(212_992, 131_072);
        assert_eq!(
            Direction::Receive.describe(Some(DATACENTER), DATACENTER, Ok(&limits)),
            "TCP receive buffers of data connections: requested at 16.0 MiB, which the kernel \
             grants as 416.0 KiB, more than the 128.0 KiB it keeps them at by itself"
        );
        let limits = without_receive_growth(212_992, 16 * MIB);
        assert_eq!(
            Direction::Receive.describe(None, DATACENTER, Ok(&limits)),
            "TCP receive buffers of data connections: left to the kernel, which keeps them at \
             16.0 MiB; a request for 16.0 MiB would be held at 416.0 KiB"
        );
        // send buffers always grow, whatever that setting says
        assert_eq!(
            Direction::Send.describe(None, DATACENTER, Ok(&limits)),
            "TCP send buffers of data connections: left to the kernel, which grows them to \
             4.0 MiB; a request for 16.0 MiB would be held at 416.0 KiB"
        );
    }

    /// A log event as a test sees it: its level, its target and its message.
    type Logged = (tracing::Level, String, String);

    /// Every log event `body` emits, with no filter in the way.
    fn events_of(body: impl FnOnce()) -> Vec<Logged> {
        #[derive(Default)]
        struct Recorder(std::sync::Mutex<Vec<Logged>>);
        struct Message(String);
        impl tracing::field::Visit for Message {
            fn record_debug(&mut self, field: &tracing::field::Field, value: &dyn std::fmt::Debug) {
                if field.name() == "message" {
                    self.0 = format!("{value:?}");
                }
            }
        }
        impl tracing::Subscriber for Recorder {
            fn enabled(&self, _: &tracing::Metadata<'_>) -> bool {
                true
            }
            fn new_span(&self, _: &tracing::span::Attributes<'_>) -> tracing::span::Id {
                tracing::span::Id::from_u64(1)
            }
            fn record(&self, _: &tracing::span::Id, _: &tracing::span::Record<'_>) {}
            fn record_follows_from(&self, _: &tracing::span::Id, _: &tracing::span::Id) {}
            fn event(&self, event: &tracing::Event<'_>) {
                let mut message = Message(String::new());
                event.record(&mut message);
                let metadata = event.metadata();
                self.0.lock().unwrap().push((
                    *metadata.level(),
                    metadata.target().to_owned(),
                    message.0,
                ));
            }
            fn enter(&self, _: &tracing::span::Id) {}
            fn exit(&self, _: &tracing::span::Id) {}
        }
        let recorder = std::sync::Arc::new(Recorder::default());
        tracing::subscriber::with_default(recorder.clone(), body);
        recorder.0.lock().unwrap().clone()
    }

    // rcp shows errors and notices unless it is asked for more with -v. the decision is detail for
    // those who ask; the warning is for the user who does not know there is anything to ask about
    #[test]
    fn the_destination_warns_a_starved_host_on_the_channel_shown_without_verbosity() {
        let starved = without_receive_growth(212_992, 131_072);
        let events = events_of(|| announce_receive(Some(DATACENTER), DATACENTER, Ok(&starved)));
        let [decision, warning] = events.as_slice() else {
            panic!("expected one decision and one warning, got {events:?}");
        };
        assert_eq!(decision.0, tracing::Level::INFO);
        assert_ne!(decision.1, common::NOTICE_TARGET, "{decision:?}");
        assert!(
            decision.2.starts_with("TCP receive buffers"),
            "{decision:?}"
        );
        assert_eq!(warning.0, tracing::Level::WARN);
        assert_eq!(warning.1, common::NOTICE_TARGET, "{warning:?}");
        assert!(
            warning.2.contains("do not grow on this host"),
            "{warning:?}"
        );
    }

    #[test]
    fn the_destination_does_not_warn_on_a_host_with_stock_settings() {
        let (_, stock) = hosts()[0];
        let events = events_of(|| {
            announce_receive(None, DATACENTER, Ok(&stock));
            announce_receive(
                None,
                DATACENTER,
                Err("kernel setting ipv4/tcp_rmem: unreadable"),
            );
        });
        assert_eq!(events.len(), 2, "expected two decisions, got {events:?}");
        for (level, target, _) in &events {
            assert_eq!(*level, tracing::Level::INFO, "{events:?}");
            assert_ne!(target, common::NOTICE_TARGET, "{events:?}");
        }
    }

    #[test]
    fn each_end_plans_from_the_limits_of_the_host_it_runs_on() {
        let limits = common::host::current().tcp_buffers().ok();
        for profile in [
            crate::NetworkProfile::Datacenter,
            crate::NetworkProfile::Internet,
        ] {
            let size = profile.socket_buffer_size();
            assert_eq!(
                SendBufferRequest::for_this_host(profile),
                SendBufferRequest::plan(size, limits),
                "{profile:?}"
            );
            assert_eq!(
                ReceiveBufferRequest::for_this_host(profile),
                ReceiveBufferRequest::plan(size, limits),
                "{profile:?}"
            );
        }
    }
}
