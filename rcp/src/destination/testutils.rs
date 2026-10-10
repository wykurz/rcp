pub(super) fn copy_settings(
    overwrite: bool,
    remote_copy_buffer_size: usize,
) -> common::copy::Settings {
    common::copy::Settings {
        reflink: Default::default(),
        local_copy_handoff: Default::default(),
        dereference: false,
        fail_early: false,
        overwrite,
        overwrite_compare: Default::default(),
        overwrite_filter: None,
        ignore_existing: false,
        chunk_size: 0,
        skip_specials: false,
        remote_copy_buffer_size,
        filter: None,
        dry_run: None,
        delete: None,
    }
}

pub(super) struct ResetAdmission;

impl Drop for ResetAdmission {
    fn drop(&mut self) {
        throttle::set_admission_limits(None);
    }
}
