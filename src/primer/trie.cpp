#include "primer/trie.h"
#include <string_view>
#include "common/exception.h"
#include <stack>
#include <typeinfo>
#include <iostream>

namespace bustub {

template <class T>
auto Trie::Get(std::string_view key) const -> const T * {
  // throw NotImplementedException("Trie::Get is not implemented.");

  // You should walk through the trie to find the node corresponding to the key. If the node doesn't exist, return
  // nullptr. After you find the node, you should use `dynamic_cast` to cast it to `const TrieNodeWithValue<T> *`. If
  // dynamic_cast returns `nullptr`, it means the type of the value is mismatched, and you should return nullptr.
  // Otherwise, return the value.
  if (root_ == nullptr) {
    return nullptr;
  }
  auto new_node = &root_;
  for (char keys : key) {
    auto find_ = (*new_node)->children_.find(keys);
    if (find_ != (*new_node)->children_.end()){
      new_node = &((*new_node)->children_.at(keys));
    }else{
      return nullptr;
    }

  }
  if((*new_node)->is_value_node_){
    if(dynamic_cast<const TrieNodeWithValue<T> *>((*new_node).get()) == nullptr){
      return nullptr;
    }
    return dynamic_cast<const TrieNodeWithValue<T> *>((*new_node).get())->value_.get();
  } 
  return nullptr;

}

template <class T>
auto Trie::Put(std::string_view key, T value) const -> Trie {
  // Note that `T` might be a non-copyable type. Always use `std::move` when creating `shared_ptr` on that value.
  // throw NotImplementedException("Trie::Put is not implemented.");

  // You should walk through the trie and create new nodes if necessary. If the node corresponding to the key already
  // exists, you should create a new `TrieNodeWithValue`.
  
  // Clone the new root
  // std::cout<<"Hello"<<std::endl;
  auto new_root = std::make_unique<TrieNode>(TrieNode(std::map<char, std::shared_ptr<const TrieNode>>()));
  if (root_ != nullptr){
    new_root = root_->Clone();
  }

  auto root_with_value = std::make_shared<T>(std::move(value));
  if (key.empty()){
    auto temp = std::make_unique<TrieNodeWithValue<T>>(TrieNodeWithValue(new_root->children_, std::move(root_with_value)));
    return Trie(std::move(temp));
  }

  std::unique_ptr<TrieNode> slot = nullptr;
  // 建立一个栈存字符和指针
  std::stack<std::pair<char, std::unique_ptr<TrieNode>>> key_stack;
  

  auto parent = &new_root;
  auto it_key = key.begin();
  
  // 便利到倒数第二个位置
  for (; it_key +1 != key.end(); it_key++ ){
    auto it_find = ((*parent) -> children_).find(*it_key);
  // 如果找到的话克隆后加入栈中
    if (it_find != ((*parent)->children_).end()){
      slot = ((*parent)->children_).at(*it_key)->Clone();
      key_stack.push(std::make_pair(*it_key, std::move(slot)));
      parent = &key_stack.top().second;  // 裸指针很危险，如果还是parent=&slot，会出问题
    }else{
      break;
    }
  }
  // 栈存的是能够便利到的key的node

  // 加上不能便利到的key的node
  while(it_key +1 != key.end()){
    auto tep = std::make_unique<TrieNode>(TrieNode(std::map<char, std::shared_ptr<const TrieNode>>()));
    key_stack.push(std::make_pair(*it_key, std::move(tep)));
    it_key++;
  }

  slot = std::make_unique<TrieNodeWithValue<T>>(TrieNodeWithValue(std::map<char, std::shared_ptr<const TrieNode>>(), std::move(root_with_value)));
  char key_slot = *it_key;
  while( !key_stack.empty()){
    auto if_find = key_stack.top().second->children_.find(key_slot);
    if (if_find != key_stack.top().second->children_.end()){
      if(it_key + 1 == key.end() ){
        slot -> children_ = key_stack.top().second->children_.at(key_slot)->children_;
        --it_key;
      }
      key_stack.top().second->children_.at(key_slot) = std::move(slot);
    } else{
      it_key = key.begin();
      key_stack.top().second->children_.insert(std::make_pair(key_slot, std::move(slot)));
    }

    key_slot = key_stack.top().first;
    slot = std::move(key_stack.top().second);
    key_stack.pop();
  }

  auto it_find = new_root->children_.find(key_slot);
  if (it_find != new_root->children_.end()) {
    new_root->children_.at(key_slot) = std::move(slot);
  } else {
    new_root->children_.insert(std::make_pair(key_slot, std::move(slot)));
  }

  return Trie(std::shared_ptr<const TrieNode>(std::move(new_root)));

}


auto Trie::Remove(std::string_view key) const -> Trie {
  // throw NotImplementedException("Trie::Remove is not implemented.");
  // You should walk through the trie and remove nodes if necessary. If the node doesn't contain a value any more,
  // you should convert it to `TrieNode`. If a node doesn't have children any more, you should remove it.
  if (key.empty()){
    return Trie(nullptr);
  }
  
  if (root_ == nullptr) {
    return Trie(nullptr);
  }

  auto new_root = root_->Clone();
  std::stack<std::pair<char, std::unique_ptr<TrieNode>>> stack_vec;
  stack_vec.push(std::make_pair('a', std::move(new_root)));

  auto it_key = key.begin();
  while (it_key != key.end()) {
    // 遍历key 如果找到就把节点压进去
    auto it_find = (stack_vec.top().second)->children_.find(*it_key);
    if (it_find != (stack_vec.top().second)->children_.end()) {
      auto slot = (stack_vec.top().second)->children_.at(*it_key)->Clone();
      stack_vec.push(std::make_pair(*it_key, std::move(slot)));
    
    } else {
      // 遍历key 如果遇到一个不存在的直接返回root
      new_root = root_->Clone();
      return Trie(std::shared_ptr<const TrieNode>(std::move(new_root)));
    }
    it_key++;
  }
  // 判断带不带value，非叶子节点如果不带value不会删
  if ((stack_vec.top().second)->is_value_node_) {  // 不带value的非终端结点不可能删，终端结点都带VALUE
    char char_key = stack_vec.top().first;
    if (stack_vec.top().second->children_.empty()) {
      stack_vec.pop();
      stack_vec.top().second->children_.erase(char_key);
      while (stack_vec.size() > 1) {  // 直到root或第一个带VALUE的非终端结点
        if (stack_vec.top().second->children_.empty() && !(stack_vec.top().second->is_value_node_)) {
          char_key = stack_vec.top().first;
          stack_vec.pop();
          stack_vec.top().second->children_.erase(char_key);
        } else {
          break;
        }
      }
    } else {
      auto slot = std::move(stack_vec.top().second);
      stack_vec.pop();
      stack_vec.top().second->children_.at(char_key) = std::make_unique<TrieNode>(TrieNode(slot->children_));
    }
  }
  // 建新树
  auto slot = std::move(stack_vec.top().second);
  char key_char = stack_vec.top().first;
  stack_vec.pop();
  while (!stack_vec.empty()) {
    stack_vec.top().second->children_.at(key_char) = std::move(slot);
    key_char = stack_vec.top().first;
    slot = std::move(stack_vec.top().second);
    stack_vec.pop();
  }
  new_root = std::move(slot);
  if (!new_root->is_value_node_ && new_root->children_.empty()) {
    return Trie(nullptr);
  }

  return Trie(std::shared_ptr<const TrieNode>(std::move(new_root)));
}


// Below are explicit instantiation of template functions.
//
// Generally people would write the implementation of template classes and functions in the header file. However, we
// separate the implementation into a .cpp file to make things clearer. In order to make the compiler know the
// implementation of the template functions, we need to explicitly instantiate them here, so that they can be picked up
// by the linker.

template auto Trie::Put(std::string_view key, uint32_t value) const -> Trie;
template auto Trie::Get(std::string_view key) const -> const uint32_t *;

template auto Trie::Put(std::string_view key, uint64_t value) const -> Trie;
template auto Trie::Get(std::string_view key) const -> const uint64_t *;

template auto Trie::Put(std::string_view key, std::string value) const -> Trie;
template auto Trie::Get(std::string_view key) const -> const std::string *;

// If your solution cannot compile for non-copy tests, you can remove the below lines to get partial score.

using Integer = std::unique_ptr<uint32_t>;

template auto Trie::Put(std::string_view key, Integer value) const -> Trie;
template auto Trie::Get(std::string_view key) const -> const Integer *;

template auto Trie::Put(std::string_view key, MoveBlocked value) const -> Trie;
template auto Trie::Get(std::string_view key) const -> const MoveBlocked *;

}  // namespace bustub
