// Copyright 2019 Intel Corporation. All Rights Reserved.
// Copyright 2019-2021 Alibaba Cloud. All rights reserved.
//
// SPDX-License-Identifier: Apache-2.0

use std::collections::HashSet;
use std::fmt::{Display, Formatter};
use std::io::{self, ErrorKind, Result};
use std::marker::PhantomData;
use std::os::fd::IntoRawFd;
use std::os::unix::io::{AsRawFd, RawFd};
use std::sync::Mutex;

use mio::event::Event;
use mio::unix::SourceFd;
use mio::{Events, Interest, Poll, Registry, Token};
use vmm_sys_util::event::{
    new_event_consumer_and_notifier, EventConsumer, EventFlag, EventNotifier,
};

use super::backend::VhostUserBackend;
use super::vring::VringT;

/// Errors related to vring epoll/kqueue event handling.
#[derive(Debug)]
pub enum VringPollError {
    /// Failed to create epoll file descriptor.
    PollerCreate(io::Error),
    /// Failed while waiting for events.
    PollerWait(io::Error),
    /// Could not register exit event
    RegisterExitEvent(io::Error),
    /// Failed to read the event from kick EventFd.
    HandleEventReadKick(io::Error),
    /// Failed to handle the event from the backend.
    HandleEventBackendHandling(io::Error),
    /// Failed to clone registry.
    RegistryClone(io::Error),
    /// Vring index out or range.
    VringIndexOutOfRange(usize),
    /// Failed to create event consumer and notifier pair.
    NewEventConsumerNotifier(io::Error),
    /// Could not register vring enabled event fd.
    RegisterVringEnabledEvent(io::Error),
    /// Failed to consume event from vring enabled event fd.
    HandleVringEnabledEventConsume(io::Error),
}

impl Display for VringPollError {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        match self {
            VringPollError::PollerCreate(e) => write!(f, "cannot create poller: {e}"),
            VringPollError::PollerWait(e) => write!(f, "failed to wait for poller event: {e}"),
            VringPollError::RegisterExitEvent(e) => write!(f, "cannot register exit event: {e}"),
            VringPollError::HandleEventReadKick(e) => {
                write!(f, "cannot read vring kick event: {e}")
            }
            VringPollError::HandleEventBackendHandling(e) => {
                write!(f, "failed to handle poll event: {e}")
            }
            VringPollError::RegistryClone(e) => write!(f, "cannot clone poller's registry: {e}"),
            VringPollError::VringIndexOutOfRange(idx) => {
                write!(f, "vring index out of range: {idx}")
            }
            VringPollError::NewEventConsumerNotifier(e) => {
                write!(f, "failed to create event consumer and notifier: {e}")
            }
            VringPollError::RegisterVringEnabledEvent(e) => {
                write!(f, "cannot register vring enabled event fd: {e}")
            }
            VringPollError::HandleVringEnabledEventConsume(e) => {
                write!(f, "failed to consume vring enabled event: {e}")
            }
        }
    }
}

impl std::error::Error for VringPollError {}

/// Result of vring epoll/kqueue operations.
pub type VringPollResult<T> = std::result::Result<T, VringPollError>;

#[derive(Debug, Clone, Copy)]
pub enum EventSet {
    Readable,
    Writable,
    All,
}

impl EventSet {
    fn to_interest(self) -> Interest {
        match self {
            EventSet::Readable => Interest::READABLE,
            EventSet::Writable => Interest::WRITABLE,
            EventSet::All => Interest::READABLE | Interest::WRITABLE,
        }
    }
}

fn event_to_event_set(evt: &Event) -> Result<EventSet> {
    if evt.is_readable() && evt.is_writable() {
        Ok(EventSet::All)
    } else if evt.is_readable() {
        Ok(EventSet::Readable)
    } else if evt.is_writable() {
        Ok(EventSet::Writable)
    } else if evt.is_read_closed() {
        Err(io::Error::other("Event Err is read closed"))
    } else if evt.is_write_closed() {
        Err(io::Error::other("Event Err is write closed"))
    } else if evt.is_error() {
        Err(io::Error::other("Event Epoll Error"))
    } else {
        Err(io::Error::other("Unknown or unhandled event state"))
    }
}

/// Epoll/kqueue event handler to manage and process epoll/kqueue events for registered file descriptor.
///
/// The `VringPollHandler` structure provides interfaces to:
/// - add file descriptors to be monitored by the epoll/kqueue fd
/// - remove registered file descriptors from the epoll/kqueue fd
/// - run the event loop to handle pending events on the epoll/kqueue fd
pub struct VringPollHandler<T: VhostUserBackend> {
    registry: Registry,
    // Record the registered fd.
    // Because in mio, consecutive calls to register is unspecified behavior.
    fd_set: Mutex<HashSet<RawFd>>,
    backend: T,
    num_backend_queues: usize,
    vrings: Vec<T::Vring>,
    thread_id: usize,
    exit_event_fd: Option<EventNotifier>,
    vring_enabled_event_fd: (EventConsumer, EventNotifier),
    vrings_with_pending_events: Mutex<HashSet<usize>>,
    phantom: PhantomData<T::Bitmap>,
}

impl<T: VhostUserBackend> VringPollHandler<T> {
    /// Send `exit event` to break the event loop.
    pub fn send_exit_event(&self) {
        if let Some(eventfd) = self.exit_event_fd.as_ref() {
            let _ = eventfd.notify();
        }
    }
}

impl<T> VringPollHandler<T>
where
    T: VhostUserBackend,
{
    /// Create a `VringPollHandler` instance.
    pub(crate) fn new(
        backend: T,
        vrings: Vec<T::Vring>,
        thread_id: usize,
        poller: &Poll,
    ) -> VringPollResult<Self> {
        let num_backend_queues = backend.num_queues();
        let exit_event_fd = backend.exit_event(thread_id);
        let fd_set = Mutex::new(HashSet::new());

        let registry = poller
            .registry()
            .try_clone()
            .map_err(VringPollError::RegistryClone)?;
        let exit_event_fd = if let Some((consumer, notifier)) = exit_event_fd {
            let id = num_backend_queues;

            registry
                .register(
                    &mut SourceFd(&consumer.as_raw_fd()),
                    Token(id),
                    Interest::READABLE,
                )
                .map_err(VringPollError::RegisterExitEvent)?;

            fd_set.lock().unwrap().insert(consumer.into_raw_fd());
            Some(notifier)
        } else {
            None
        };

        let vring_enabled_event_fd =
            new_event_consumer_and_notifier(EventFlag::NONBLOCK | EventFlag::CLOEXEC)
                .map_err(VringPollError::NewEventConsumerNotifier)?;
        registry
            .register(
                &mut SourceFd(&vring_enabled_event_fd.0.as_raw_fd()),
                Token(num_backend_queues),
                Interest::READABLE,
            )
            .map_err(VringPollError::RegisterVringEnabledEvent)?;
        fd_set
            .lock()
            .unwrap()
            .insert(vring_enabled_event_fd.0.as_raw_fd());

        Ok(VringPollHandler {
            registry,
            fd_set,
            backend,
            num_backend_queues,
            vrings,
            thread_id,
            exit_event_fd,
            vring_enabled_event_fd,
            vrings_with_pending_events: Mutex::new(HashSet::new()),
            phantom: PhantomData,
        })
    }

    /// Register an event into the epoll/kqueue fd.
    ///
    /// When this event is later triggered, the backend implementation of `handle_event` will be
    /// called.
    pub fn register_listener(&self, fd: RawFd, ev_type: EventSet, queue_idx: usize) -> Result<()> {
        // `data` range [0...num_queues] is reserved for queues and exit event.
        if queue_idx <= self.num_backend_queues {
            Err(io::Error::from_raw_os_error(libc::EINVAL))
        } else {
            self.register_event(fd, ev_type, queue_idx)
        }
    }

    /// Unregister an event from the epoll/kqueue fd.
    ///
    /// If the event is triggered after this function has been called, the event will be silently
    /// dropped.
    pub fn unregister_listener(&self, fd: RawFd, queue_idx: usize) -> Result<()> {
        // `data` range [0...num_queues] is reserved for queues and exit event.
        if queue_idx <= self.num_backend_queues {
            Err(io::Error::from_raw_os_error(libc::EINVAL))
        } else {
            self.unregister_event(fd, queue_idx)
        }
    }

    pub(crate) fn register_event(
        &self,
        fd: RawFd,
        ev_type: EventSet,
        queue_idx: usize,
    ) -> Result<()> {
        let mut fd_set = self.fd_set.lock().unwrap();
        if fd_set.contains(&fd) {
            return Err(io::Error::from_raw_os_error(libc::EEXIST));
        }
        self.registry
            .register(&mut SourceFd(&fd), Token(queue_idx), ev_type.to_interest())
            .map_err(std::io::Error::other)?;
        fd_set.insert(fd);
        drop(fd_set);
        if queue_idx < self.num_backend_queues {
            self.vrings_with_pending_events
                .lock()
                .unwrap()
                .insert(queue_idx);
            self.vring_enabled_event_fd.1.notify()?;
        }
        Ok(())
    }

    pub(crate) fn unregister_event(&self, fd: RawFd, queue_idx: usize) -> Result<()> {
        if queue_idx < self.num_backend_queues {
            self.vrings_with_pending_events
                .lock()
                .unwrap()
                .remove(&queue_idx);
        }
        let mut fd_set = self.fd_set.lock().unwrap();
        if !fd_set.contains(&fd) {
            return Err(io::Error::from_raw_os_error(libc::ENOENT));
        }
        self.registry
            .deregister(&mut SourceFd(&fd))
            .map_err(|e| std::io::Error::other(format!("Failed to deregister fd {fd}: {e}")))?;
        fd_set.remove(&fd);
        Ok(())
    }

    /// Run the event poll loop to handle all pending events on registered fds.
    ///
    /// The event loop will be terminated once an event is received from the `exit event fd`
    /// associated with the backend.
    pub(crate) fn run(&self, mut poller: Poll) -> VringPollResult<()> {
        const POLL_EVENTS_LEN: usize = 100;

        let mut events = Events::with_capacity(POLL_EVENTS_LEN);
        'poll: loop {
            poller
                .poll(&mut events, None)
                .map_err(VringPollError::PollerWait)?;

            for event in &events {
                let token = event.token();

                match event_to_event_set(event) {
                    Ok(evt_set) => {
                        if self.handle_event(token.0, evt_set)? {
                            break 'poll;
                        }
                    }
                    Err(err) => {
                        log::warn!(
                            "Ignoring unknown event set for token {:#x} error: {:?}",
                            token.0,
                            err
                        );
                    }
                };
            }
        }

        Ok(())
    }

    fn handle_event(&self, device_event: usize, evset: EventSet) -> VringPollResult<bool> {
        if device_event == self.num_backend_queues {
            return self.handle_num_queues_event(evset);
        }

        if device_event < self.vrings.len() {
            let vring = &self.vrings[device_event];
            let enabled = vring
                .read_kick()
                .map_err(VringPollError::HandleEventReadKick)?;

            // If the vring is not enabled, it should not be processed.
            if !enabled {
                return Ok(false);
            }
        }

        self.backend
            .handle_event(device_event, evset, &self.vrings, self.thread_id)
            .map_err(VringPollError::HandleEventBackendHandling)?;

        Ok(false)
    }

    fn handle_num_queues_event(&self, evset: EventSet) -> VringPollResult<bool> {
        // The num_queues event can be triggered by the exit event fd or a vring becoming enabled.
        // The vring_enabled_event_fd is guaranteed to be non-blocking so reading from it will
        // determine which fd triggered the event.
        match self.vring_enabled_event_fd.0.consume() {
            Ok(()) => {
                let vrings_with_pending_events =
                    std::mem::take(&mut *self.vrings_with_pending_events.lock().unwrap());
                for device_event in vrings_with_pending_events {
                    if device_event >= self.vrings.len() {
                        return Err(VringPollError::VringIndexOutOfRange(device_event));
                    }
                    self.backend
                        .handle_event(device_event, evset, &self.vrings, self.thread_id)
                        .map_err(VringPollError::HandleEventBackendHandling)?;
                }
                // There may have been a notification in the exit fd too, but it wasn't consumed and
                // it's not safe to attempt to because that fd may not be non-blocking. Returning
                // false ensures there will be another iteration of the main loop which will trigger
                // the same event if exit fd is readable.
                Ok(false)
            }
            // With no notification in the vring enabled fd the only other option is the exit fd.
            Err(e) if e.kind() == ErrorKind::WouldBlock => Ok(self.exit_event_fd.is_some()),
            Err(e) => Err(VringPollError::HandleVringEnabledEventConsume(e)),
        }
    }
}

impl<T: VhostUserBackend> AsRawFd for VringPollHandler<T> {
    fn as_raw_fd(&self) -> RawFd {
        self.registry.as_raw_fd()
    }
}

#[cfg(test)]
mod tests {
    use super::super::backend::tests::MockVhostBackend;
    use super::super::vring::VringRwLock;
    use super::*;
    use std::sync::{mpsc, Arc, Mutex};
    use std::time::Duration;
    use vm_memory::{GuestAddress, GuestMemoryAtomic, GuestMemoryMmap};
    use vmm_sys_util::event::{new_event_consumer_and_notifier, EventFlag};

    #[test]
    fn test_register_event_does_not_wait_for_backend() {
        let mem = GuestMemoryAtomic::new(
            GuestMemoryMmap::<()>::from_ranges(&[(GuestAddress(0x100000), 0x10000)]).unwrap(),
        );
        let vring = VringRwLock::new(mem, 0x1000).unwrap();
        let backend = Arc::new(Mutex::new(MockVhostBackend::new()));
        let poller = Poll::new().unwrap();
        let handler = Arc::new(
            VringPollHandler::new(backend.clone(), vec![vring.clone()], 0, &poller).unwrap(),
        );
        let (consumer, _notifier) = new_event_consumer_and_notifier(EventFlag::NONBLOCK).unwrap();
        let backend_guard = backend.lock().unwrap();
        let (done_tx, done_rx) = mpsc::channel();

        let registration = std::thread::spawn(move || {
            let _vring_guard = vring.get_ref();
            let result = handler.register_event(consumer.as_raw_fd(), EventSet::Readable, 0);
            done_tx.send(result).unwrap();
        });

        let result = done_rx.recv_timeout(Duration::from_secs(1));
        drop(backend_guard);
        registration.join().unwrap();

        result
            .expect("registering a vring must not wait for the backend lock")
            .unwrap();
    }

    #[test]
    fn test_vring_poll_handler() {
        let mem = GuestMemoryAtomic::new(
            GuestMemoryMmap::<()>::from_ranges(&[(GuestAddress(0x100000), 0x10000)]).unwrap(),
        );
        let vring = VringRwLock::new(mem, 0x1000).unwrap();
        let backend = Arc::new(Mutex::new(MockVhostBackend::new()));

        let poller = Poll::new().unwrap();
        let handler = VringPollHandler::new(backend, vec![vring], 0x1, &poller).unwrap();

        let (consumer, _notifier) = new_event_consumer_and_notifier(EventFlag::empty()).unwrap();
        handler
            .register_listener(consumer.as_raw_fd(), EventSet::Readable, 3)
            .unwrap();
        // Register an already registered fd.
        handler
            .register_listener(consumer.as_raw_fd(), EventSet::Readable, 3)
            .unwrap_err();
        // Register an invalid data.
        handler
            .register_listener(consumer.as_raw_fd(), EventSet::Readable, 1)
            .unwrap_err();

        handler
            .unregister_listener(consumer.as_raw_fd(), 3)
            .unwrap();
        // unregister an already unregistered fd.
        handler
            .unregister_listener(consumer.as_raw_fd(), 3)
            .unwrap_err();
        // unregister an invalid data.
        handler
            .unregister_listener(consumer.as_raw_fd(), 1)
            .unwrap_err();
        // Validate registry fd
        assert_ne!(handler.as_raw_fd(), -1);
        assert_ne!(handler.as_raw_fd(), poller.as_raw_fd());
    }
}
