use omnipaxos_kv::common::kv::KVCommand;
use std::collections::HashMap;

pub struct Database {
    db: HashMap<String, String>,
}

impl Database {
    pub fn new() -> Self {
        Self { db: HashMap::new() }
    }

    /// 返回值含义：
    ///   None                    → 写操作成功（Put / Delete / Cas 成功）
    ///   Some(Some(value))       → 读操作，key 存在
    ///   Some(None)              → 读操作，key 不存在
    ///   Some(Some("__cas_failed__")) → Cas precondition 不匹配
    pub fn handle_command(&mut self, command: KVCommand) -> Option<Option<String>> {
        match command {
            KVCommand::Put(key, value) => {
                self.db.insert(key, value);
                None
            }
            KVCommand::Delete(key) => {
                self.db.remove(&key);
                None
            }
            KVCommand::Get(key) => {
                Some(self.db.get(&key).cloned())
            }
            KVCommand::Cas(key, expected, new_value) => {
                let current = self.db.get(&key).cloned().unwrap_or_default();
                if current == expected {
                    self.db.insert(key, new_value);
                    None  // 成功，和 Put 一样返回 None
                } else {
                    // precondition 不匹配，用特殊标记告知 server
                    Some(Some("__cas_failed__".to_string()))
                }
            }
        }
    }
}
