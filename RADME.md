
## TODO

### 🔧 TODO: Implement `UNKNOWN_MEMBER_ID` checks

- [ ] **JoinGroupRequest**  
  Validate that returning members are known to the group. Reject unknown members.

- [ ] **SyncGroupRequest**  
  Ensure only registered group members can participate in synchronization. Reject others.

- [ ] **HeartbeatRequest**  
  Return `UNKNOWN_MEMBER_ID` if the member is not present in the group.

- [ ] **LeaveGroupRequest**  
  Reject the request if the member is no longer part of the group.

- [ ] **OffsetCommitRequest**  
  Require a valid, active group member to commit offsets.

- [ ] **TxnOffsetCommitRequest**  
  Same validation rules as OffsetCommitRequest.

---
