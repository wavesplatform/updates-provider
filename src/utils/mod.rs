pub mod chunks {
    pub struct Chunks<I: Iterator> {
        iter: Option<I>,
        chunk_size: usize,
    }

    impl<I: Iterator> Iterator for Chunks<I> {
        type Item = Vec<I::Item>;

        fn next(&mut self) -> Option<Self::Item> {
            if let Some(ref mut iter) = self.iter {
                let mut chunk = Vec::with_capacity(self.chunk_size);
                loop {
                    if chunk.len() == self.chunk_size {
                        return Some(chunk);
                    }
                    if let Some(item) = iter.next() {
                        chunk.push(item);
                    } else {
                        self.iter = None;
                        return Some(chunk);
                    }
                }
            }
            None
        }
    }

    pub trait ToChunks: Iterator + Sized {
        fn chunks_from_iter(self, chunk_size: usize) -> Chunks<Self> {
            assert!(chunk_size > 0);
            Chunks {
                iter: Some(self),
                chunk_size,
            }
        }
    }

    impl<I: Iterator> ToChunks for I {}

    #[test]
    fn chunks_test() {
        let vals = vec![1, 2, 3, 4, 5];
        let result = vals.into_iter().chunks_from_iter(2).collect::<Vec<_>>();
        let expected = vec![1, 2];
        assert_eq!(result.get(0), Some(&expected));
        let expected = vec![3, 4];
        assert_eq!(result.get(1), Some(&expected));
        let expected = vec![5];
        assert_eq!(result.get(2), Some(&expected));
    }

    #[test]
    #[should_panic]
    fn chunks_invalid_size_test() {
        let vals = vec![1, 2];
        let _ = vals.into_iter().chunks_from_iter(0).collect::<Vec<_>>();
    }
}
