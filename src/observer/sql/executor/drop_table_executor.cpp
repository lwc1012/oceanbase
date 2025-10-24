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
// Created by Wangyunlai on 2023/4/25.
//

#include "sql/executor/drop_table_executor.h"
#include "common/log/log.h"
#include "event/sql_event.h"
#include "sql/stmt/drop_table_stmt.h"
#include "storage/default/default_handler.h"

RC DropTableExecutor::execute(SQLStageEvent *sql_event)
{
  RC rc = RC::SUCCESS;
  Stmt *stmt = sql_event->stmt();

  // 确保是DROP_TABLE类型的语句
  if (stmt->type() != StmtType::DROP_TABLE) {
    LOG_WARN("drop table executor can't handle stmt type: %d", static_cast<int>(stmt->type()));
    return RC::INTERNAL;
  }

  // 转换为DropTableStmt
  DropTableStmt *drop_table_stmt = static_cast<DropTableStmt *>(stmt);
  const char *table_name = drop_table_stmt->table_name().c_str();

  // 获取数据库实例
  Db *db = sql_event->session_event()->session()->get_current_db();
  if (db == nullptr) {
    LOG_WARN("no database selected");
    return RC::SCHEMA_DB_NOT_SELECTED;
  }

  // 获取处理器并执行删除表操作
  DefaultHandler *handler = DefaultHandler::get_instance();
  rc = handler->drop_table(db->name(), table_name);
  if (rc != RC::SUCCESS) {
    LOG_WARN("failed to drop table. table=%s, rc=%d:%s", table_name, rc, strrc(rc));
    return rc;
  }

  LOG_INFO("successfully drop table: %s", table_name);
  return rc;
}