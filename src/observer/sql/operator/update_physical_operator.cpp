/* Copyright (c) 2021 OceanBase and/or its affiliates. All rights reserved.
miniob is licensed under Mulan PSL v2.
You can use this software according to the terms and conditions of the Mulan PSL v2.
You may obtain a copy of Mulan PSL v2 at:
         http://license.coscl.org.cn/MulanPSL2
THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
See the Mulan PSL v2 for more details. */

#include "common/log/log.h"
#include "sql/operator/update_physical_operator.h"
#include "sql/expr/expression.h"
#include "sql/expr/tuple.h"
#include "sql/expr/field_expr.h"
#include "sql/stmt/filter_stmt.h"
#include "sql/expr/expression_evaluator.h"
#include "storage/field/field.h"
#include "storage/table/table.h"
#include "storage/record/record.h"
#include "storage/trx/trx.h"
#include "storage/index/index.h"
#include "sql/operator/table_scan_physical_operator.h"

UpdatePhysicalOperator::UpdatePhysicalOperator(Trx *trx, UpdateStmt *update_stmt)
  : trx_(trx), update_stmt_(update_stmt), affected_rows_(0)
{}

RC UpdatePhysicalOperator::open(Trx *trx)
{
  if (nullptr != trx) {
    trx_ = trx;
  }

  // 打开子操作符
  PhysicalOperator *child = first_child();
  if (nullptr != child) {
    return child->open(trx_);
  }
  return RC::SUCCESS;
}

RC UpdatePhysicalOperator::next()
{
  PhysicalOperator *child = first_child();
  if (nullptr == child) {
    return RC::RECORD_EOF;
  }

  Table *table = update_stmt_->table();
  const FieldMeta *field_meta = update_stmt_->field_meta();
  const Value &value = update_stmt_->value();

  RC rc = child->next();
  if (rc != RC::SUCCESS) {
    return rc;
  }

  // 获取当前记录
  Tuple *tuple = child->current_tuple();
  if (nullptr == tuple) {
    LOG_WARN("failed to get current tuple");
    return RC::INTERNAL;
  }

  // 尝试获取Record对象
  TableScanPhysicalOperator *table_scan = dynamic_cast<TableScanPhysicalOperator *>(child);
  Record *record = nullptr;
  if (table_scan != nullptr) {
    record = table_scan->current_record();
  } else {
    // 尝试从RowTuple获取记录
    RowTuple *row_tuple = dynamic_cast<RowTuple *>(tuple);
    if (row_tuple != nullptr) {
      record = row_tuple->record();
    }
  }

  if (nullptr == record) {
    LOG_WARN("failed to get record");
    return RC::INTERNAL;
  }

  // 先获取更新前的字段值，用于删除旧的索引项
  Value old_value;
  rc = record->get_value(field_meta, old_value);
  if (rc != RC::SUCCESS) {
    LOG_WARN("failed to get old value. rc=%d", rc);
    return rc;
  }

  // 如果值没有变化，跳过更新
  if (old_value.compare(value) == 0) {
    return next();
  }

  // 更新记录中的字段值
  rc = record->set_value(field_meta, value);
  if (rc != RC::SUCCESS) {
    LOG_WARN("failed to set value. rc=%d", rc);
    return rc;
  }

  // 处理索引更新
  const vector<Index *> &indexes = table->indexes();
  for (Index *index : indexes) {
    // 只处理包含当前字段的索引
    const FieldMeta *index_field_meta = index->field_meta();
    if (index_field_meta->field_id() == field_meta->field_id()) {
      // 从索引中删除旧值
      rc = index->delete_entry(old_value, record->rid());
      if (rc != RC::SUCCESS) {
        LOG_WARN("failed to delete old index entry. rc=%d", rc);
        return rc;
      }
      // 向索引中插入新值
      rc = index->insert_entry(value, record->rid());
      if (rc != RC::SUCCESS) {
        LOG_WARN("failed to insert new index entry. rc=%d", rc);
        return rc;
      }
    }
  }

  affected_rows_++;
  return next();
}

RC UpdatePhysicalOperator::close()
{
  PhysicalOperator *child = first_child();
  if (nullptr != child) {
    return child->close();
  }
  return RC::SUCCESS;
}