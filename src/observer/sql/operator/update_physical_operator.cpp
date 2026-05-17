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
// Created by (your name) on 2024.
//

#include "sql/operator/update_physical_operator.h"
#include "common/log/log.h"
#include "common/lang/string.h"
#include "storage/table/table.h"
#include "storage/trx/trx.h"
#include "storage/field/field_meta.h"

UpdatePhysicalOperator::UpdatePhysicalOperator(Table *table, const FieldMeta *field_meta, const Value &value)
    : table_(table), field_meta_(field_meta), value_(value)
{}

RC UpdatePhysicalOperator::open(Trx *trx)
{
  if (children_.empty()) {
    return RC::SUCCESS;
  }

  unique_ptr<PhysicalOperator> &child = children_[0];
  
  RC rc = child->open(trx);
  if (rc != RC::SUCCESS) {
    LOG_WARN("failed to open child operator: %s", strrc(rc));
    return rc;
  }

  trx_ = trx;

  // 第一步：收集所有需要更新的行
  while (OB_SUCC(rc = child->next())) {
    Tuple *tuple = child->current_tuple();
    if (nullptr == tuple) {
      LOG_WARN("failed to get current record: %s", strrc(rc));
      return rc;
    }

    RowTuple *row_tuple = static_cast<RowTuple *>(tuple);
    Record   &record    = row_tuple->record();
    Record    record_copy;
    rc = record_copy.copy_data(record.data(), record.len());
    if (OB_FAIL(rc)) {
      LOG_WARN("failed to copy record data. rc=%s", strrc(rc));
      return rc;
    }
    record_copy.set_rid(record.rid());
    records_.emplace_back(std::move(record_copy));
  }

  rc = child->close();
  if (rc != RC::SUCCESS) {
    LOG_WARN("failed to close child operator: %s", strrc(rc));
    return rc;
  }

  // 第二步：对每一行执行更新（先删旧行，再插新行）
  for (const Record &record : records_) {
    // 复制旧记录的数据，构造新记录
    Record new_record;
    rc = new_record.copy_data(record.data(), record.len());
    if (rc != RC::SUCCESS) {
      LOG_WARN("failed to copy record for update. rc=%s", strrc(rc));
      return rc;
    }
    new_record.set_rid(record.rid());

    Value real_value;
    if (value_.attr_type() == field_meta_->type()) {
      real_value = value_;
    } else {
      rc = Value::cast_to(value_, field_meta_->type(), real_value);
      if (OB_FAIL(rc)) {
        LOG_WARN("failed to cast value for update. rc=%s", strrc(rc));
        return rc;
      }
    }

    // 将新值写入新记录中目标字段的位置
    // new_record.data() 返回字节数组起始地址，字段按固定长度顺序排列
    const int field_offset = field_meta_->offset();
    const int field_len    = field_meta_->len();
    memcpy(new_record.data() + field_offset, real_value.data(), field_len);
    // 通过事务执行更新（先删除旧记录，再插入新记录）
    rc = trx_->update_record(table_, const_cast<Record &>(record), new_record);
    if (rc != RC::SUCCESS) {
      LOG_WARN("failed to update record. rc=%s", strrc(rc));
      return rc;
    }
  }

  return RC::SUCCESS;
}

RC UpdatePhysicalOperator::next()
{
  return RC::RECORD_EOF;
}

RC UpdatePhysicalOperator::close()
{
  return RC::SUCCESS;
}