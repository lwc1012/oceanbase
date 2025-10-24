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

#include "common/log/log.h"
#include "common/types.h"
#include "sql/stmt/update_stmt.h"
#include "sql/stmt/filter_stmt.h"
#include "storage/db/db.h"
#include "storage/table/table.h"
#include "event/sql_debug.h"

UpdateStmt::UpdateStmt(Table *table, const FieldMeta *field_meta, const Value &value, FilterStmt *filter_stmt)
    : table_(table), field_meta_(field_meta), value_(value), filter_stmt_(filter_stmt)
{}

UpdateStmt::~UpdateStmt()
{
  if (filter_stmt_ != nullptr) {
    delete filter_stmt_;
    filter_stmt_ = nullptr;
  }
}

RC UpdateStmt::create(Db *db, const UpdateSqlNode &update, Stmt *&stmt)
{
  // 检查数据库是否存在
  if (nullptr == db) {
    LOG_WARN("invalid argument. db is null");
    return RC::INVALID_ARGUMENT;
  }

  // 获取表名
  const string &table_name = update.relation_name;
  if (table_name.empty()) {
    LOG_WARN("invalid argument. relation name is empty");
    return RC::INVALID_ARGUMENT;
  }

  // 查找表
  Table *table = db->find_table(table_name.c_str());
  if (nullptr == table) {
    LOG_WARN("no such table. db=%s, table_name=%s", db->name(), table_name.c_str());
    return RC::SCHEMA_TABLE_NOT_EXIST;
  }

  // 查找字段
  const string &attr_name = update.attribute_name;
  const FieldMeta *field_meta = table->table_meta().field(attr_name.c_str());
  if (nullptr == field_meta) {
    LOG_WARN("no such field. table=%s, field=%s", table_name.c_str(), attr_name.c_str());
    return RC::SCHEMA_FIELD_NOT_EXIST;
  }

  // 检查值类型是否匹配
  const Value &value = update.value;
  if (value.attr_type() != field_meta->type() && value.attr_type() != AttrType::UNDEFINED) {
    LOG_WARN("field type mismatch. table=%s, field=%s, expected=%d, actual=%d", 
             table_name.c_str(), attr_name.c_str(), field_meta->type(), value.attr_type());
    return RC::SCHEMA_FIELD_TYPE_MISMATCH;
  }

  // 处理过滤条件
  FilterStmt *filter_stmt = nullptr;
  RC rc = FilterStmt::create(db, table, update.conditions, filter_stmt);
  if (rc != RC::SUCCESS) {
    LOG_WARN("failed to create filter statement. rc=%d", rc);
    return rc;
  }

  // 创建UpdateStmt对象
  stmt = new UpdateStmt(table, field_meta, value, filter_stmt);
  sql_debug("update statement: table=%s, field=%s, value=%s", 
            table_name.c_str(), attr_name.c_str(), value.to_string().c_str());
  return RC::SUCCESS;
}
