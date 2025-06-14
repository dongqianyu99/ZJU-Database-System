#include "index/b_plus_tree.h"

#include "common/instance.h"
#include "gtest/gtest.h"
#include "index/comparator.h"
#include "utils/tree_file_mgr.h"
#include "utils/utils.h"

#include <fstream>

static const std::string db_name = "bp_tree_insert_test.db";

TEST(BPlusTreeTests, SampleTest) {
  // Init engine
  DBStorageEngine engine(db_name);
  std::vector<Column *> columns = {
      new Column("int", TypeId::kTypeInt, 0, false, false),
  };
  Schema *table_schema = new Schema(columns);
  KeyManager KP(table_schema, 17);
  BPlusTree tree(0, engine.bpm_, KP);
  TreeFileManagers mgr("tree_");
  // Prepare data
  const int n = 2000;
  vector<GenericKey *> keys;
  vector<RowId> values;
  vector<GenericKey *> delete_seq;
  map<GenericKey *, RowId> kv_map;
  for (int i = 0; i < n; i++) {
    GenericKey *key = KP.InitKey();
    std::vector<Field> fields{Field(TypeId::kTypeInt, i)};
    KP.SerializeFromKey(key, Row(fields), table_schema);
    keys.push_back(key);
    values.push_back(RowId(i));
    delete_seq.push_back(key);
  }
  vector<GenericKey *> keys_copy(keys);
  // Shuffle data
  // ShuffleArray(keys);
  // ShuffleArray(values);
  // ShuffleArray(delete_seq);
  // Map key value
  for (int i = 0; i < n; i++) {
    kv_map[keys[i]] = values[i];
  }

  // std::cout << "Map key value done." << endl;

  // Insert data
  for (int i = 0; i < n; i++) {
    tree.Insert(keys[i], values[i]);
    // std::ofstream out("tree.dot");
    // if (out.is_open()) {
    //     tree.PrintTree(out, table_schema);  // 调用函数
    //     out.close();                     // 关闭文件
    // } else {
    //     std::cerr << "Failed to open output file." << std::endl;
    // }
  }
  ASSERT_TRUE(tree.Check());
  // Page *check_page = tree.FindLeafPage(keys[4], INVALID_PAGE_ID, false);
  // auto *check_node = reinterpret_cast<LeafPage *>(check_page->GetData());

  
  // Print tree
  tree.PrintTree(mgr[0], table_schema);
  // std::ofstream out("tree.dot");  // 打开输出文件
  // if (out.is_open()) {
  //     tree.PrintTree(out, table_schema);  // 调用函数
  //     out.close();                     // 关闭文件
  // } else {
  //     std::cerr << "Failed to open output file." << std::endl;
  // }

  // Search keys
  vector<RowId> ans;
  for (int i = 0; i < n; i++) {
    // std::cout << "i = " << i << std::endl;
    tree.GetValue(keys_copy[i], ans);
    ASSERT_EQ(kv_map[keys_copy[i]], ans[i]);
  }
  ASSERT_TRUE(tree.Check());
  // Delete half keys
  for (int i = 0; i < n / 2; i++) {
    tree.Remove(delete_seq[i]);
    // std::ofstream out("tree.dot");  // 打开输出文件
    // if (out.is_open()) {
    //     tree.PrintTree(out, table_schema);  // 调用函数
    //     out.close();                     // 关闭文件
    // } else {
    //     std::cerr << "Failed to open output file." << std::endl;
    // }

  }
  tree.PrintTree(mgr[1], table_schema);
  // Check valid
  ans.clear();
  for (int i = 0; i < n / 2; i++) {
    ASSERT_FALSE(tree.GetValue(delete_seq[i], ans));
  }
  for (int i = n / 2; i < n; i++) {
    ASSERT_TRUE(tree.GetValue(delete_seq[i], ans));
    ASSERT_EQ(kv_map[delete_seq[i]], ans[ans.size() - 1]);
  }
}