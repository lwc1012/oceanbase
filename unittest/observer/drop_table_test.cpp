/* Copyright (c) 2021 OceanBase and/or its affiliates. All rights reserved.
miniob is licensed under Mulan PSL v2.
You can use this software according to the terms and conditions of the Mulan PSL v2.
You may obtain a copy of Mulan PSL v2 at:
         http://license.coscl.org.cn/MulanPSL2
THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
MERCHANTABILITY OR FITNESS FOR A PARTICULAR PURPOSE.
See the Mulan PSL v2 for more details. */

// Created by GitHub Copilot on 2026/05/16.

#include <filesystem>
#include <memory>
#include <vector>
#include <string>

#include "gtest/gtest.h"
#include "storage/db/db.h"
#include "storage/table/table.h"
#include "storage/common/meta_util.h"

using namespace std;
using namespace common;

TEST(DropTable, basic)
{
  filesystem::path test_directory("drop_table_test");
  filesystem::remove_all(test_directory);
  filesystem::create_directory(test_directory);

  const char *dbname = "test_db";
  filesystem::path db_path = test_directory / dbname;
  filesystem::create_directories(db_path);

  auto db = make_unique<Db>();
  ASSERT_EQ(RC::SUCCESS, db->init(dbname, db_path.c_str(), "mvcc", "disk"));

  const char *table_name = "t1";
  vector<AttrInfoSqlNode> attr_infos;
  for (int i = 0; i < 3; i++) {
    AttrInfoSqlNode a;
    a.name = string("f") + to_string(i);
    a.type = AttrType::INTS;
    a.length = 4;
    attr_infos.push_back(a);
  }

  // create table
  ASSERT_EQ(RC::SUCCESS, db->create_table(table_name, attr_infos, {}));
  ASSERT_NE(nullptr, db->find_table(table_name));

  // sync to flush meta
  ASSERT_EQ(RC::SUCCESS, db->sync());

  // check meta file exists
  string meta_file = table_meta_file(db->path().c_str(), table_name);
  ASSERT_TRUE(filesystem::exists(meta_file));

  // drop table
  ASSERT_EQ(RC::SUCCESS, db->drop_table(table_name));

  // after drop, table should not be found
  ASSERT_EQ(nullptr, db->find_table(table_name));

  // meta file and data file should be removed (or at least not exist)
  string data_file = table_data_file(db->path().c_str(), table_name);
  ASSERT_FALSE(filesystem::exists(meta_file));
  ASSERT_FALSE(filesystem::exists(data_file));

  filesystem::remove_all(test_directory);
}
