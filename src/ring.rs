//! The producer's side of the shared ring, layout 1: a frame is published into the slot its sequence
//! selects, and every reader waiting on the shared word is woken. The ring is memory the Core created and
//! handed over as a descriptor, so mapping it is the one place this crate needs `unsafe`.

use std::os::fd::OwnedFd;
use std::sync::atomic::{AtomicU32, AtomicU64, Ordering};

use rustix::mm::{MapFlags, ProtFlags, mmap, munmap};
use rustix::thread::futex;

const HEADER: usize = 64;
const SLOT_HEADER: usize = 32;
const MAGIC: &[u8; 8] = b"YOKERING";
const VERSION: u32 = 1;
const OFF_SLOTS: usize = 12;
const OFF_CAPACITY: usize = 16;
const OFF_PUBLISHED: usize = 24;
const OFF_WAKE: usize = 32;

/// Publishes into a ring the producer was handed.
pub(crate) struct Writer {
    mem: *mut u8,
    len: usize,
    slots: u32,
    capacity: u32,
    published: u64,
}

// The mapping is private to the writer, which is held under the stream's lock.
unsafe impl Send for Writer {}

impl Writer {
    /// Maps the ring a descriptor names, for writing, and reads its geometry from its header.
    pub(crate) fn open(fd: &OwnedFd) -> Result<Writer, String> {
        let len = rustix::fs::fstat(fd).map_err(|e| e.to_string())?.st_size as usize;
        if len < HEADER {
            return Err(format!("the ring is {len} bytes, shorter than its header"));
        }
        let mem = unsafe {
            mmap(
                std::ptr::null_mut(),
                len,
                ProtFlags::READ | ProtFlags::WRITE,
                MapFlags::SHARED,
                fd,
                0,
            )
        }
        .map_err(|e| e.to_string())? as *mut u8;
        let word = |at: usize| u32::from_le(unsafe { (mem.add(at) as *const u32).read() });
        let (slots, capacity) = (word(OFF_SLOTS), word(OFF_CAPACITY));
        let magic = unsafe { std::slice::from_raw_parts(mem, 8) };
        let fault = if magic != MAGIC {
            Some("the memory handed is not a ring".to_string())
        } else if word(8) != VERSION {
            Some(format!(
                "the ring is of layout {}; this library writes layout {VERSION}",
                word(8)
            ))
        } else if slots == 0
            || HEADER as u64 + slots as u64 * (SLOT_HEADER as u64 + capacity as u64) > len as u64
        {
            Some(format!(
                "the ring's geometry, {slots} slots of {capacity} bytes, does not fit its {len} bytes"
            ))
        } else {
            None
        };
        if let Some(fault) = fault {
            let _ = unsafe { munmap(mem.cast(), len) };
            return Err(fault);
        }
        let published =
            unsafe { AtomicU64::from_ptr(mem.add(OFF_PUBLISHED).cast()) }.load(Ordering::Acquire);
        Ok(Writer {
            mem,
            len,
            slots,
            capacity,
            published,
        })
    }

    /// Writes one frame and wakes every waiter. A frame larger than a slot is refused.
    pub(crate) fn publish(&mut self, clock: u64, payload: &[u8]) -> Result<(), String> {
        if payload.len() > self.capacity as usize {
            return Err(format!(
                "the frame is {} bytes, and a slot of this ring holds {}",
                payload.len(),
                self.capacity
            ));
        }
        let n = self.published + 1;
        let at = HEADER
            + ((n - 1) % self.slots as u64) as usize * (SLOT_HEADER + self.capacity as usize);
        unsafe {
            let sequence = AtomicU64::from_ptr(self.mem.add(at).cast());
            sequence.store(0, Ordering::Release);
            (self.mem.add(at + 8) as *mut u64).write_unaligned(clock.to_le());
            (self.mem.add(at + 16) as *mut u32).write_unaligned((payload.len() as u32).to_le());
            std::ptr::copy_nonoverlapping(
                payload.as_ptr(),
                self.mem.add(at + SLOT_HEADER),
                payload.len(),
            );
            sequence.store(n, Ordering::Release);
            AtomicU64::from_ptr(self.mem.add(OFF_PUBLISHED).cast()).store(n, Ordering::Release);
            let wake = AtomicU32::from_ptr(self.mem.add(OFF_WAKE).cast());
            wake.fetch_add(1, Ordering::AcqRel);
            // A reader in another process waits on this word, so the wake is shared and not private.
            let _ = futex::wake(wake, futex::Flags::empty(), i32::MAX as u32);
        }
        self.published = n;
        Ok(())
    }
}

impl Drop for Writer {
    fn drop(&mut self) {
        let _ = unsafe { munmap(self.mem.cast(), self.len) };
    }
}
