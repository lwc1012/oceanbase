/* Copyright (c) 2021 OceanBase and/or its affiliates. All rights reserved.
miniob is licensed under Mulan PSL v2.
You can use this software according to the terms and conditions of the Mulan PSL v2.
You may obtain a copy of Mulan PSL v2 at:
         http://license.coscl.org.cn/MulanPSL2
THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
See the Mulan PSL v2 for more details. */

//
// Created by Wangyunlai on 2022/5/22.
//

#include "sql/stmt/drop_table_stmt.h"

#include "common/log/log.h"
#include "common/lang/string.h"
#include "storage/db/db.h"
#include "storage/table/table.h"

RC DropTableStmt::create(Db *db, DropTableSqlNode &drop_table, Stmt *&stmt)
{
  if (nullptr == db) {
    LOG_WARN("Database not selected");
    return RC::SCHEMA_DB_NOT_SELECTED;
  }

  if (common::is_blank(drop_table.relation_name)) {
    LOG_WARN("Invalid table name");
    return RC::INVALID_ARGUMENT;
  }

  // 检查表是否存在
  // 注意：这里不需要检查表是否存在，因为即使表不存在，我们也应该返回相应的错误码
  // 实际的表存在性检查会在执行器中进行

  stmt = new DropTableStmt(drop_table.relation_name);
  LOG_INFO("Create drop table statement for table: %s", drop_table.relation_name);
  return RC::SUCCESS;
}