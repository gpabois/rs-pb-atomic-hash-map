use std::{alloc::{alloc, dealloc, Layout}, hash::{Hash, Hasher as _}, ptr::NonNull, sync::{atomic::{AtomicUsize, Ordering}, Arc}};

use pb_atomic_linked_list::{prelude::AtomicLinkedList as _, AtomicLinkedList};


fn hash<K: Hash>(key: &K) -> usize {
    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    key.hash(&mut hasher);
    let hashed_key = hasher.finish() as usize;
    hashed_key
}

pub struct Entry<K: Hash,V> {
    pub key: K,
    pub value: V
}

impl<K: Hash, V> Entry<K,V> {
    pub fn hash(&self) -> usize {
        hash(&self.key)
    }
}

struct Bucket<K: Hash,V>(AtomicLinkedList<Entry<K,V>>);

impl<K: Hash, V> Bucket<K, V> {
    pub fn insert(&mut self, key: K, value: V) -> bool {
        unsafe {
            let exists = self.get_raw_entry_ptr(&key).is_some();
            self.0.insert(Entry{key, value});
            !exists
        }
    }

    unsafe fn get_raw_entry_ptr(&self, key: &K) -> Option<NonNull<Entry<K,V>>> {
        let hashed_key = hash(key);
        self.0.iter()
        .map(|rf| std::ptr::from_ref(rf) as *mut Entry<K,V>)
        .map(|ptr| NonNull::new_unchecked(ptr))
        .filter(|ptr| ptr.as_ref().hash() == hashed_key)
        .last()
    }
}

impl<K: Hash, V> Bucket<K, V> {
    fn new() -> Self {
        Self(AtomicLinkedList::new())
    }
}

/// A hash table
struct Table<K: Hash,V> {
    buckets: NonNull<Bucket<K,V>>,
    buckets_layout: Layout,
    capacity: usize,
    length: AtomicUsize
}

impl<K: Hash, V> Drop for Table<K,V> {
    fn drop(&mut self) {
        unsafe {
            for i in 0..self.capacity {
                self
                    .buckets
                    .add(i)
                    .drop_in_place();
            }

            dealloc(self.buckets.cast().as_ptr(), self.buckets_layout);
        }
    }
}

impl<K: Hash,V> Table<K,V> {
    pub fn new(capacity: usize) -> Self {       
        let buckets_layout = Layout::array::<Bucket<K,V>>(capacity).unwrap();

        unsafe {
            let buckets: NonNull<Bucket<K, V>> = NonNull::new(
                alloc(buckets_layout)
                .cast::<Bucket<K,V>>()
            ).unwrap();

            // Initialise all buckets.
            for i in 0..capacity {
                let bucket_ptr = buckets.add(i);
                *bucket_ptr.as_ptr() = Bucket::new();
            }

            let length = AtomicUsize::new(0);

            Self {
                buckets, 
                buckets_layout,
                capacity, 
                length
            }
        }
        
    }

    pub fn len(&self) -> usize {
        return self.length.load(Ordering::Relaxed)
    }

    /// Insert a new value in the bucket.
    pub fn insert(&self, key: K, value: V) {
        let hashed_key = hash(&key);
        let bucket_key = hashed_key % self.capacity;
        unsafe {
            let bucket = self.buckets.add(bucket_key).as_mut();
            if bucket.insert(key, value) {
                self.length.fetch_add(1, Ordering::Relaxed);
            }
        }
    }

    /// Retrieve the bucket which might contain the value behind the key.
    unsafe fn get_bucket(&self, key: &K) -> NonNull<Bucket<K,V>> {
        let mut hasher = std::collections::hash_map::DefaultHasher::new();
        key.hash(&mut hasher);
        let hashed_key = hasher.finish() as usize;
        let bucket_key = hashed_key % self.capacity;
        return self.buckets.add(bucket_key)
    }


    unsafe fn get_raw_entry_ptr(&self, key: &K) -> Option<NonNull<Entry<K,V>>> {
        self.get_bucket(key).as_ref().get_raw_entry_ptr(key)
    }
}

pub struct AtomicHashMap<K: Hash, V>(Arc<Table<K, V>>);

impl<K: Hash, V> Clone for AtomicHashMap<K, V> {
    fn clone(&self) -> Self {
        Self(self.0.clone())
    }
}

impl<K: Hash, V> AtomicHashMap<K, V> {
    /// Creates a new atomic hash map
    pub fn new(capacity: usize) -> Self {
        Self(Arc::new(Table::new(capacity)))
    }

    /// Returns the length 
    pub fn len(&self) -> usize {
        self.0.len()
    }

    /// Insert a new value 
    /// 
    /// If a value is already stored in the hash map,
    /// this will not overwrite its content, the new version will
    /// appended in the linked list.
    pub fn insert(&mut self, key: K, value: V) {
        self.0.insert(key, value);
    }

    /// Borrow the value behind the key
    /// 
    /// Always returns the last version in the linked list.
    pub fn borrow<'a>(&'a self, key: &K) -> Option<&'a V> {
        unsafe {
            self.get_raw_value_ptr(key).map(|value: NonNull<V>| value.as_ref())
        }
    }

    /// Get the raw pointer to the value.
    /// 
    /// # Unsafe
    /// Because of obvious reasons
    pub unsafe fn get_raw_value_ptr(&self, key: &K) -> Option<NonNull<V>> {
        self.0.get_raw_entry_ptr(key)
        .map(|mut entry| NonNull::new_unchecked(std::ptr::from_mut(&mut entry.as_mut().value)))
    }
}

#[cfg(test)]
mod tests {
    use crate::AtomicHashMap;

    #[test]
    fn test_borrow_unexisting_value() {
        let map = AtomicHashMap::<u32, u32>::new(10);
        assert_eq!(map.borrow(&10).copied(), None);
    }
}