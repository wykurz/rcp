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
//! socket connects: on kernels before 6.11 a later request no longer widens the window the socket
//! may advertise, which then stays at its initial 64 KiB however large the buffer is. Only the
//! function that configures a socket before it connects accepts a [`ReceiveBufferRequest`]. That
//! function also takes the kernel's "fixed" mark off the buffer again before the socket connects:
//! the size stays, but a buffer still marked at that moment would narrow the connection's window
//! scale to half the buffer.
//!
//! Outside tests, a request for a size only comes out of [`SendBufferRequest::for_this_host`] and
//! [`ReceiveBufferRequest::for_this_host`], so a size that the limits of this host do not call for
//! cannot reach a socket.

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
    /// runs on. The decision and the figures behind it are logged once.
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
        if requested.is_some() {
            format!(
                "TCP {direction} buffers of data connections: requested at {wanted}, which the \
                 kernel grants as {granted}, more than the {growth} it grows them to by itself"
            )
        } else {
            format!(
                "TCP {direction} buffers of data connections: left to the kernel, which grows \
                 them to {growth}; a request for {wanted} would be held at {granted}"
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
                limits(212_992, 4 * MIB, 131_072),
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
