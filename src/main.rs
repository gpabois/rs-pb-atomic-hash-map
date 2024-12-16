use std::{alloc::{alloc, Layout}, hash::{Hash, Hasher as _}, ptr::NonNull, sync::atomic::{AtomicUsize, Ordering}};

use pb_atomic_linked_list::{prelude::AtomicLinkedList as _, AtomicLinkedList};

pub type EntryList<K,V> = AtomicLinkedList<Entry<K,V>>;

struct Entry<K,V> {
    key: K,
    value: V
}

struct Bucket<K: Hash,V> {
    base: NonNull<AtomicLinkedList<Entry<K, V>>>,
    key: usize,
    size: usize
}

impl<K: Hash,V> Bucket<K, V> {
    /// Creates a new bucket.
    pub unsafe fn new(base: NonNull<EntryList<K,V>>, key: usize, size: usize) -> Self {
        let bucket = Self {base, key, size};
        bucket.initialise();
        bucket
    }
    
    /// Insert an entry in the bucket.
    pub fn insert(&self, key: K, value: V) {
        unsafe {
            self.get_entries_list(&key).as_mut().insert(Entry{key, value});
        }
    }

    unsafe fn get_entries_list(&self, key: &K) -> NonNull<EntryList<K,V>> {
        let mut hasher = std::collections::hash_map::DefaultHasher::new();
        key.hash(&mut hasher);
        let hashed_key = hasher.finish() as usize;
        let entry_key = hashed_key % self.size;
        self.base.add(entry_key)
    }
    

    /// Initialise the bucket entries lists
    /// 
    /// # Unsafe
    /// If initialise is called twice, it would overwrite any prior written content.
    unsafe fn initialise(&self) {
        // initialise the bucket
        for i in 0..self.size {
            *self.base.add(i).as_mut() = EntryList::new();
        }
    }
}

/// A hash table
pub struct Table<K: Hash,V> {
    buckets: NonNull<Bucket<K,V>>,
    capacity: usize,
    bucket_size: usize,
    length: AtomicUsize
}

impl<K: Hash,V> Table<K,V> {
    pub fn new(capacity: usize, bucket_size: usize) -> Self {       
        let buckets_layout = Layout::array::<Bucket<K,V>>(capacity).unwrap();
        let entries_lists_layout = Layout::array::<AtomicLinkedList<V>>(capacity * bucket_size).unwrap();

        unsafe {
            let buckets= NonNull::new(alloc(buckets_layout).cast::<Bucket<K,V>>()).unwrap();
            let entries_lists = NonNull::new(alloc(entries_lists_layout).cast::<EntryList<K,V>>()).unwrap();

            // Initialise all buckets.
            for i in 0..capacity {
                let mut bucket_ptr = buckets.add(i);
                *bucket_ptr.as_mut() = Bucket::new(entries_lists.add(i), i, bucket_size);
            }

            let length = AtomicUsize::new(0);

            Self {
                buckets, 
                bucket_size, 
                capacity, 
                length
            }
        }
        
    }

    pub fn len(&self) -> usize {
        return self.length.load(Ordering::Relaxed)
    }

    pub fn insert(&mut self, key: K, value: V) {
        let mut hasher = std::collections::hash_map::DefaultHasher::new();
        key.hash(&mut hasher);
        let hashed_key = hasher.finish() as usize;
        let bucket_key = hashed_key % self.capacity;
        unsafe {
            let bucket = self.buckets.add(bucket_key).as_mut();
            bucket.insert(key, value);
            self.length.fetch_add(1, Ordering::Relaxed);
        }
    }

    unsafe fn get_bucket(&self, key: &K) -> NonNull<Bucket<K,V>> {
        let mut hasher = std::collections::hash_map::DefaultHasher::new();
        key.hash(&mut hasher);
        let hashed_key = hasher.finish() as usize;
        let bucket_key = hashed_key % self.capacity;
        return self.buckets.add(bucket_key)
    }


    unsafe fn get_entry(&self, key: &K) -> Option<NonNull<Entry<K,V>>> {
        self.get_bucket(key).get_entry
    }
}