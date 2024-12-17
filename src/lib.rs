use std::{alloc::{alloc, dealloc, Layout}, borrow::Borrow, hash::{Hash, Hasher as _}, marker::PhantomData, mem::MaybeUninit, ops::Deref, ptr::NonNull, sync::{atomic::{AtomicUsize, Ordering}, Arc}};

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
    buckets: NonNull<[Bucket<K,V>]>,
    buckets_layout: Layout,
    capacity: usize,
    length: AtomicUsize
}

unsafe impl<K: Hash,V> Sync for Table<K,V> {}
unsafe impl<K: Hash,V> Send for Table<K,V> {}

impl<K: Hash, V> Drop for Table<K,V> {
    fn drop(&mut self) {
        unsafe {
            for bucket in self.buckets.as_mut().iter_mut() {
                std::ptr::from_mut(bucket).drop_in_place();
            }

            dealloc(self.buckets.cast().as_ptr(), self.buckets_layout);
        }
    }
}

impl<K: Hash,V> Table<K,V> {
    pub fn new(capacity: usize) -> Self {       
        let buckets_layout = Layout::array::<Bucket<K,V>>(capacity).unwrap();

        unsafe {
            let raw_buckets_base_ptr = alloc(buckets_layout).cast::<MaybeUninit<Bucket<K,V>>>();
            let raw_buckets_slice_ptr= std::ptr::slice_from_raw_parts_mut(
                raw_buckets_base_ptr,
                capacity
            );

            if let Some(mut_buckets_slice_ptr) = raw_buckets_slice_ptr.as_mut() {
                // Initialise all buckets.
                for bucket in mut_buckets_slice_ptr {
                    bucket.write(Bucket::new());
                }
            }

            let buckets = NonNull::new(std::mem::transmute(raw_buckets_slice_ptr)).unwrap();
   
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
        unsafe {
            let bucket = self.get_bucket_ptr(&key).as_mut();
            if bucket.insert(key, value) {
                self.length.fetch_add(1, Ordering::Relaxed);
            }
        }
    }

    /// Retrieve the bucket which might contain the value behind the key.
    unsafe fn get_bucket_ptr(&self, key: &K) -> NonNull<Bucket<K,V>> {
        let mut hasher = std::collections::hash_map::DefaultHasher::new();
        key.hash(&mut hasher);
        let hashed_key = hasher.finish() as usize;
        let bucket_key = hashed_key % self.capacity;
        let bucket_ptr = std::ptr::from_mut(
            self
                .buckets
                .as_ptr()
                .as_mut()
                .unwrap()
                .get_mut(bucket_key)
                .unwrap()
        );

        return NonNull::new(bucket_ptr).unwrap()
    }


    unsafe fn get_raw_entry_ptr(&self, key: &K) -> Option<NonNull<Entry<K,V>>> {
        self
            .get_bucket_ptr(key)
            .as_ref()
            .get_raw_entry_ptr(key)
    }
}

pub struct ValueIter<'a, K: Hash + 'a, V: 'a>(EntryIter<'a, K, V>);

impl<'a, K: Hash + 'a, V: 'a> Iterator for ValueIter<'a, K, V> {
    type Item = &'a V;

    fn next(&mut self) -> Option<Self::Item> {
        self.0.next().map(|entry| &entry.value)
    }
}


pub struct Iter<'a, K: Hash + 'a, V: 'a>(EntryIter<'a, K, V>);

impl<'a, K: Hash + 'a, V: 'a> Iterator for Iter<'a, K, V> {
    type Item = (&'a K, &'a V);

    fn next(&mut self) -> Option<Self::Item> {
        self.0.next().map(|entry| (&entry.key, &entry.value))
    }
}

struct EntryIter<'a, K: Hash + 'a, V: 'a> {
    _phantom: PhantomData<&'a ()>,
    buckets: NonNull<[Bucket<K, V>]>,
    current_bucket_iter: Option<pb_atomic_linked_list::Iter<'a, Entry<K, V>>>,
    current_bucket_key: usize
}

impl<'a, K: Hash + 'a, V: 'a> Iterator for EntryIter<'a, K, V> {
    type Item = &'a Entry<K, V>;

    fn next(&mut self) -> Option<Self::Item> {
        unsafe {
            if let Some(iter) = &mut self.current_bucket_iter {
                if let Some(entry) = iter.next() {
                    return Some(entry)
                }
                else {
                    self.current_bucket_key += 1;
                    if self.current_bucket_key >= self.buckets.len() {
                        return None
                    }                    
                    let bucket = self.buckets.as_ref().get(self.current_bucket_key).unwrap();
                    self.current_bucket_iter = Some(bucket.0.iter());
                    self.next()
                }

            } else {
                return None
            }
        }
    }
}

#[derive(Debug, PartialEq, Eq)]
pub struct ValueRef<'a, T>(&'a T);

impl<'a, T: Clone> ValueRef<'a, T> {
    pub fn to_owned(value: Self) -> T {
        value.0.clone()
    }
}

impl<'a, T> Deref for ValueRef<'a, T> {
    type Target = T;
    
    fn deref(&self) -> &Self::Target {
        self.0
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
        Self(
            Arc::new(
                Table::new(capacity)
            )
        )
    }

    fn iter_entries(&self) -> EntryIter<'_, K, V> {
        unsafe {
            EntryIter {
                _phantom: PhantomData,
                buckets: self.0.buckets,
                current_bucket_iter: self.0.buckets.as_ref().get(0).map(|bucket| bucket.0.iter()),
                current_bucket_key: 0  
            }
        }
    }

    /// Iterate over all values in the hash map
    pub fn iter_values(&self) -> ValueIter<'_, K, V> {
        ValueIter(self.iter_entries())
    }

    /// Iterate over all key/value pair in the hash map 
    pub fn iter(&self) -> Iter<'_, K, V> {
        Iter(self.iter_entries())
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
    pub fn borrow<'a, Q: Borrow<K>>(&'a self, key: Q) -> Option<ValueRef<'a, V>> {
        unsafe {
            self
            .get_raw_value_ptr(key.borrow())
            .map(|value| value.as_ref())
            .map(ValueRef)
        }
    }

    /// Get the raw pointer to the value.
    /// 
    /// # Unsafe
    /// Because of obvious reasons
    pub unsafe fn get_raw_value_ptr(&self, key: &K) -> Option<NonNull<V>> {
        self.0
        .get_raw_entry_ptr(key)
        .map(|mut entry| 
            NonNull::new(
                std::ptr::from_mut(
                    &mut entry.as_mut().value
                )
            ).unwrap()
        )
    }
}

#[cfg(test)]
mod tests {
    use std::{collections::HashSet, thread};

    use crate::{AtomicHashMap, ValueRef};

    #[test]
    fn test_borrow_unexisting_value() {
        let mut map = AtomicHashMap::<u32, u32>::new(100);
        map.insert(20, 30); 
        assert_eq!(map.borrow(&10), None);
    }

    #[test]
    fn test_borrow_existing_value() {
        let mut map = AtomicHashMap::<u32, u32>::new(100);
        map.insert(10, 20); 
        assert_eq!(map.borrow(&10).map(ValueRef::to_owned), Some(20));
    }

    #[test]
    fn test_iter() {
        let mut map = AtomicHashMap::<u32, u32>::new(100);
        let mut expected_entries = HashSet::<(u32, u32)>::default();
        for i in 0..1_000 {
            map.insert(i, i);
            expected_entries.insert((i,i));
        }

        let got = map.iter().map(|(k, v)| (*k, *v)).collect::<HashSet::<_>>();
        assert_eq!(got, expected_entries)
    }

    #[test]
    fn test_multiple_threads() {
        let map = AtomicHashMap::<u32, u32>::new(100);
        let mut map1 = map.clone();
        let mut map2 = map.clone();

        let mut expected_entries = HashSet::<(u32, u32)>::default();
        for i in 0..=20_000 {
            expected_entries.insert((i, i));
        }


        let j1 = thread::spawn(move || {
            for i in 0..=10_000 {
                map1.insert(i, i);
            }
        });

        let j2 = thread::spawn(move || {
            for i in 10_001..=20_000 {
                map2.insert(i, i);
            }
        });

        j1.join().unwrap();
        j2.join().unwrap();

        let got = map.iter().map(|(k, v)| (*k, *v)).collect::<HashSet::<_>>();
        assert_eq!(got, expected_entries)
    }
}