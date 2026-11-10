#![no_std]
#![no_main]

use core::panic::PanicInfo;

#[panic_handler]
fn panic(_: &PanicInfo) -> ! {
    loop {}
}

/// CKB-VM exit(0). Syscall 93 matches ckb-std `SYS_EXIT`.
#[no_mangle]
pub extern "C" fn _start() -> ! {
    unsafe {
        core::arch::asm!("li a0, 0", "li a7, 93", "ecall", options(noreturn));
    }
}
