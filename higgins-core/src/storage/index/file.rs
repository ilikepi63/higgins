pub struct IndexFile {
    path: String,
    file_handle: std::fs::File,
    memmap: memmap2::MmapMut,
}

impl IndexFile {}
