use std::collections::BTreeMap;

#[derive(Debug)]
pub enum BencodeValue {
    Integer(i64),
    Bytes(Vec<u8>),
    List(Vec<BencodeValue>),
    Map(BTreeMap<Vec<u8>, BencodeValue>),
}

impl From<&str> for BencodeValue {
    fn from(s: &str) -> Self {
        BencodeValue::Bytes(s.as_bytes().to_vec())
    }
}

impl From<i64> for BencodeValue {
    fn from(i: i64) -> Self {
        BencodeValue::Integer(i)
    }
}

impl BencodeValue {
    fn collect_bytes(bytes: &[u8], segments: &mut Vec<u8>) {
        segments.extend_from_slice(format!("{}:", bytes.len()).as_bytes());
        segments.extend_from_slice(bytes);
    }

    fn collect_segments(&self, segments: &mut Vec<u8>) {
        match self {
            BencodeValue::Integer(i) => segments.extend_from_slice(format!("i{}e", i).as_bytes()),
            BencodeValue::Bytes(b) => Self::collect_bytes(b, segments),
            BencodeValue::List(l) => {
                segments.extend_from_slice(b"l");
                for item in l {
                    item.collect_segments(segments);
                }
                segments.extend_from_slice(b"e");
            }
            BencodeValue::Map(m) => {
                segments.extend_from_slice(b"d");
                for (k, v) in m {
                    Self::collect_bytes(k, segments);
                    v.collect_segments(segments);
                }
                segments.extend_from_slice(b"e");
            }
        }
    }

    pub fn serialize(&self) -> Vec<u8> {
        let mut ret = vec![];
        self.collect_segments(&mut ret);
        ret
    }
}

#[cfg(test)]
mod serialization_test {
    use super::BencodeValue;
    use std::collections::BTreeMap;

    #[test]
    fn integer() {
        assert_eq!(BencodeValue::Integer(04).serialize(), b"i4e");
        assert_eq!(BencodeValue::Integer(0).serialize(), b"i0e");
        assert_eq!(BencodeValue::Integer(-0).serialize(), b"i0e");
        assert_eq!(BencodeValue::Integer(-42).serialize(), b"i-42e");
    }

    #[test]
    fn bytes() {
        assert_eq!(BencodeValue::Bytes(vec![]).serialize(), b"0:");
        assert_eq!(BencodeValue::Bytes(vec![0]).serialize(), b"1:\0");
        assert_eq!(BencodeValue::from("ascii").serialize(), b"5:ascii");
        assert_eq!(
            BencodeValue::from("\u{4e07}\u{56fd}\u{7801}").serialize(),
            b"9:\xe4\xb8\x87\xe5\x9b\xbd\xe7\xa0\x81"
        );
    }

    #[test]
    fn list() {
        assert_eq!(BencodeValue::List(vec![]).serialize(), b"le");
        assert_eq!(
            BencodeValue::List(vec![BencodeValue::Integer(42), BencodeValue::from("foo")])
                .serialize(),
            b"li42e3:fooe"
        );
        assert_eq!(
            BencodeValue::List(vec![
                BencodeValue::List(vec![]),
                BencodeValue::Map(BTreeMap::new())
            ])
            .serialize(),
            b"lledee"
        );
    }

    #[test]
    fn map() {
        assert_eq!(BencodeValue::Map(BTreeMap::new()).serialize(), b"de");

        let mut map1 = BTreeMap::new();
        map1.insert(b"foo".to_vec(), BencodeValue::Integer(42));
        map1.insert(b"bar".to_vec(), BencodeValue::from("foo"));
        assert_eq!(
            BencodeValue::Map(map1).serialize(),
            b"d3:bar3:foo3:fooi42ee"
        );

        let mut map2 = BTreeMap::new();
        map2.insert(b"buz".to_vec(), BencodeValue::List(vec![]));
        map2.insert(b"foo".to_vec(), BencodeValue::Integer(42));
        map2.insert(b"bar".to_vec(), BencodeValue::Map(BTreeMap::new()));
        assert_eq!(
            BencodeValue::Map(map2).serialize(),
            b"d3:barde3:buzle3:fooi42ee"
        );
    }
}
