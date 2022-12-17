use super::{
    BlockDevice,
    DiskInode,
    DiskInodeType,
    DirEntry,
    EasyFileSystem,
    DIRENT_SZ,
    get_block_cache,
    block_cache_sync_all,
};
use alloc::sync::Arc;
use alloc::string::String;
use alloc::vec::Vec;
use spin::{Mutex, MutexGuard};

/// Virtual filesystem layer over easy-fs
pub struct Inode {
    pub inode_id: u32,
    block_id: usize,
    block_offset: usize,
    fs: Arc<Mutex<EasyFileSystem>>,
    block_device: Arc<dyn BlockDevice>,
}

impl Inode {
    /// Create a vfs inode
    pub fn new(
        inode_id: u32,
        block_id: u32,
        block_offset: usize,
        fs: Arc<Mutex<EasyFileSystem>>,
        block_device: Arc<dyn BlockDevice>,
    ) -> Self {
        Self {
            inode_id,
            block_id: block_id as usize,
            block_offset,
            fs,
            block_device,
        }
    }
    pub fn get_type(&self) -> DiskInodeType {
        let _fs = self.fs.lock();
        self.read_disk_inode(|disk_inode| {
            disk_inode.get_type()
        })
    }
    /// Call a function over a disk inode to read it
    fn read_disk_inode<V>(&self, f: impl FnOnce(&DiskInode) -> V) -> V {
        get_block_cache(
            self.block_id,
            Arc::clone(&self.block_device)
        ).lock().read(self.block_offset, f)
    }
    /// Call a function over a disk inode to modify it
    fn modify_disk_inode<V>(&self, f: impl FnOnce(&mut DiskInode) -> V) -> V {
        get_block_cache(
            self.block_id,
            Arc::clone(&self.block_device)
        ).lock().modify(self.block_offset, f)
    }
    pub fn get_num_links(&self, inode_id: u32) -> u32 {
        let _fs = self.fs.lock();
        self.num_links_with_lock(inode_id)
    }
    /// Get number of links of inode_id
    fn num_links_with_lock(&self, inode_id: u32) -> u32 {
        self.read_disk_inode(|root_inode| {
            // assert it is a directory
            assert!(root_inode.is_dir());
            let file_count = (root_inode.size as usize) / DIRENT_SZ;
            let mut dirent = DirEntry::empty();
            let mut cnt = 0;
            for i in 0..file_count {
                assert_eq!(
                    root_inode.read_at(
                        DIRENT_SZ * i,
                        dirent.as_bytes_mut(),
                        &self.block_device,
                    ),
                    DIRENT_SZ,
                );
                if dirent.inode_number() == inode_id {
                    cnt += 1;
                }
            }
            cnt
        })
    }
    /// Find inode under a disk inode by name
    fn find_inode_id(
        &self,
        name: &str,
        disk_inode: &DiskInode,
    ) -> Option<u32> {
        // assert it is a directory
        assert!(disk_inode.is_dir());
        let file_count = (disk_inode.size as usize) / DIRENT_SZ;
        let mut dirent = DirEntry::empty();
        for i in 0..file_count {
            assert_eq!(
                disk_inode.read_at(
                    DIRENT_SZ * i,
                    dirent.as_bytes_mut(),
                    &self.block_device,
                ),
                DIRENT_SZ,
            );
            if dirent.name() == name {
                return Some(dirent.inode_number() as u32);
            }
        }
        None
    }
    fn find_inode_id_and_idx(
        &self,
        name: &str,
        disk_inode: &DiskInode,
    ) -> Option<(u32, usize)> {
        // assert it is a directory
        assert!(disk_inode.is_dir());
        let file_count = (disk_inode.size as usize) / DIRENT_SZ;
        let mut dirent = DirEntry::empty();
        for i in 0..file_count {
            assert_eq!(
                disk_inode.read_at(
                    DIRENT_SZ * i,
                    dirent.as_bytes_mut(),
                    &self.block_device,
                ),
                DIRENT_SZ,
            );
            if dirent.name() == name {
                return Some((dirent.inode_number() as u32, i));
            }
        }
        None
    }
    /// Find inode under current inode by name
    pub fn find(&self, name: &str) -> Option<Arc<Inode>> {
        let fs = self.fs.lock();
        self.read_disk_inode(|disk_inode| {
            assert!(disk_inode.is_dir());

            self.find_inode_id(name, disk_inode)
            .map(|inode_id| {
                let (block_id, block_offset) = fs.get_disk_inode_pos(inode_id);
                Arc::new(Self::new(
                    inode_id,
                    block_id,
                    block_offset,
                    self.fs.clone(),
                    self.block_device.clone(),
                ))
            })
        })
    }
    /// Increase the size of a disk inode
    fn increase_size(
        &self,
        new_size: u32,
        disk_inode: &mut DiskInode,
        fs: &mut MutexGuard<EasyFileSystem>,
    ) {
        if new_size < disk_inode.size {
            return;
        }
        let blocks_needed = disk_inode.blocks_num_needed(new_size);
        let mut v: Vec<u32> = Vec::new();
        for _ in 0..blocks_needed {
            v.push(fs.alloc_data());
        }
        disk_inode.increase_size(new_size, v, &self.block_device);
    }
    /// Create inode under current inode by name
    pub fn create(&self, name: &str) -> Option<Arc<Inode>> {
        let mut fs = self.fs.lock();
        if self.read_disk_inode(|root_inode| {
            // assert it is a directory
            assert!(root_inode.is_dir());
            // has the file been created?
            self.find_inode_id(name, root_inode)
        }).is_some() {
            return None;
        }
        // create a new file
        // alloc a inode with an indirect block
        let new_inode_id = fs.alloc_inode();
        // initialize inode
        let (new_inode_block_id, new_inode_block_offset) 
            = fs.get_disk_inode_pos(new_inode_id);
        get_block_cache(
            new_inode_block_id as usize,
            Arc::clone(&self.block_device)
        ).lock().modify(new_inode_block_offset, |new_inode: &mut DiskInode| {
            new_inode.initialize(DiskInodeType::File);
        });
        self.modify_disk_inode(|root_inode| {
            assert!(root_inode.is_dir());

            // append file in the dirent
            let file_count = (root_inode.size as usize) / DIRENT_SZ;
            let new_size = (file_count + 1) * DIRENT_SZ;
            // increase size
            self.increase_size(new_size as u32, root_inode, &mut fs);
            // write dirent
            let dirent = DirEntry::new(name, new_inode_id);
            root_inode.write_at(
                file_count * DIRENT_SZ,
                dirent.as_bytes(),
                &self.block_device,
            );
        });
        log::debug!("[fs] create, inode_id = {}", new_inode_id);
        let (block_id, block_offset) = fs.get_disk_inode_pos(new_inode_id);
        block_cache_sync_all();
        // return inode
        Some(Arc::new(Self::new(
            new_inode_id,
            block_id,
            block_offset,
            self.fs.clone(),
            self.block_device.clone(),
        )))
        // release efs lock automatically by compiler
    }
    pub fn link(&self, old_name: &str, new_name: &str) -> isize {
        let mut fs = self.fs.lock();
        if self.read_disk_inode(|root_inode| {
            assert!(root_inode.is_dir());

            // has the new file been created?
            self.find_inode_id(new_name, root_inode)
        }).is_some() {
            return -1;
        }
        self.modify_disk_inode(|root_inode| {
            assert!(root_inode.is_dir());

            if let Some(inode_id) = self.find_inode_id(old_name, root_inode) {
                // append link in the dirent
                let file_count = (root_inode.size as usize) / DIRENT_SZ;
                let new_size = (file_count + 1) * DIRENT_SZ;
                // increase size
                self.increase_size(new_size as u32, root_inode, &mut fs);
                // share the same inode_id
                let dirent = DirEntry::new(new_name, inode_id);
                root_inode.write_at(
                    file_count * DIRENT_SZ,
                    dirent.as_bytes(),
                    &self.block_device,
                );
                0
            } else {
                -1
            }
        })
    }
    pub fn unlink(&self, name: &str) -> isize {
        // todo!()
        let mut _fs = self.fs.lock();
        self.modify_disk_inode(|root_inode| {
            assert!(root_inode.is_dir());

            if let Some((_inode_id, idx)) = self.find_inode_id_and_idx(name, root_inode) {
                // log::debug!("[fs] unlink: found {}, inode_id = {}, idx = {}", name, inode_id, idx);
                // remove it from dir
                let dirent = DirEntry::empty();
                // let dirent = DirEntry::new("DELETED_FILE", 0);
                root_inode.write_at(
                    idx * DIRENT_SZ,
                    dirent.as_bytes(),
                    &self.block_device,
                );
                // log::debug!("[fs] unlink: deleted from root_inode");
                0
            } else {
                -1
            }
        })
        // let inode_id = self.modify_disk_inode(|root_inode| {
        //     assert!(root_inode.is_dir());

        //     if let Some((inode_id, idx)) = self.find_inode_id_and_idx(name, root_inode) {
        //         // log::debug!("[fs] unlink: found {}, inode_id = {}, idx = {}", name, inode_id, idx);
        //         // remove it from dir
        //         let dirent = DirEntry::empty();
        //         // let dirent = DirEntry::new("DELETED_FILE", 0);
        //         root_inode.write_at(
        //             idx * DIRENT_SZ,
        //             dirent.as_bytes(),
        //             &self.block_device,
        //         );
        //         // log::debug!("[fs] unlink: deleted from root_inode");
        //         inode_id
        //     } else {
        //         0
        //     }
        // });
        // if inode_id == 0 {
        //     return -1;
        // }
        // // check if num links == 0
        // let num_links = self.num_links_with_lock(inode_id);
        // // log::debug!("[fs] unlink: num_links = {}", num_links);
        // if num_links == 0 {
        //     // log::debug!("[fs] unlink: num_links == 0, inode_id = {}", inode_id);
        //     // get inode as handle
        //     let (block_id, block_offset) = fs.get_disk_inode_pos(inode_id);
        //     let inode = Inode::new(
        //         inode_id,
        //         block_id,
        //         block_offset,
        //         self.fs.clone(),
        //         self.block_device.clone(),
        //     );
        //     // clear data
        //     inode.clear_with_lock(&mut fs);
        //     // mark inode as DELETED
        //     inode.modify_disk_inode(|disk_inode| {
        //         assert!(disk_inode.is_file());

        //         disk_inode.mark_deleted();
        //     });
        // }
        // 0
    }
    /// List inodes under current inode
    pub fn ls(&self) -> Vec<String> {
        let _fs = self.fs.lock();
        self.read_disk_inode(|disk_inode| {
            assert!(disk_inode.is_dir());

            let file_count = (disk_inode.size as usize) / DIRENT_SZ;
            let mut v: Vec<String> = Vec::new();
            for i in 0..file_count {
                let mut dirent = DirEntry::empty();
                assert_eq!(
                    disk_inode.read_at(
                        i * DIRENT_SZ,
                        dirent.as_bytes_mut(),
                        &self.block_device,
                    ),
                    DIRENT_SZ,
                );
                v.push(String::from(dirent.name()));
            }
            v
        })
    }
    /// Read data from current inode
    pub fn read_at(&self, offset: usize, buf: &mut [u8]) -> usize {
        let _fs = self.fs.lock();
        self.read_disk_inode(|disk_inode| {
            assert!(disk_inode.is_valid());

            disk_inode.read_at(offset, buf, &self.block_device)
        })
    }
    /// Write data to current inode
    pub fn write_at(&self, offset: usize, buf: &[u8]) -> usize {
        let mut fs = self.fs.lock();
        let size = self.modify_disk_inode(|disk_inode| {
            assert!(disk_inode.is_valid());

            self.increase_size((offset + buf.len()) as u32, disk_inode, &mut fs);
            disk_inode.write_at(offset, buf, &self.block_device)
        });
        block_cache_sync_all();
        size
    }
    /// Clear the data in current inode
    pub fn clear(&self) {
        log::debug!("[fs] clear data, inode_id = {}", self.inode_id);
        let mut fs = self.fs.lock();
        self.clear_with_lock(&mut fs);
    }
    fn clear_with_lock(&self, fs: &mut MutexGuard<EasyFileSystem>) {
        // log::debug!("[fs] clear data, inode_id = {}", self.inode_id);
        self.modify_disk_inode(|disk_inode| {
            assert!(disk_inode.is_file());

            let size = disk_inode.size;
            let data_blocks_dealloc = disk_inode.clear_size(&self.block_device);
            assert!(data_blocks_dealloc.len() == DiskInode::total_blocks(size) as usize);
            for data_block in data_blocks_dealloc.into_iter() {
                fs.dealloc_data(data_block);
            }
        });
        block_cache_sync_all();
    }
}
