/* Copyright (c) 2021 Xie Meiyi(xiemeiyi@hust.edu.cn) and OceanBase and/or its affiliates. All rights reserved.
miniob is licensed under Mulan PSL v2.
You can use this software according to the terms and conditions of the Mulan PSL v2.
You may obtain a copy of Mulan PSL v2 at:
         http://license.coscl.org.cn/MulanPSL2
THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
See the Mulan PSL v2 for more details. */

#include "storage/table/lsm_table_engine.h"
#include "storage/record/heap_record_scanner.h"
#include "common/log/log.h"
#include "storage/index/bplus_tree_index.h"
#include "storage/common/meta_util.h"
#include "storage/db/db.h"
#include "storage/record/lsm_record_scanner.h"
#include "storage/common/codec.h"
#include "storage/trx/lsm_mvcc_trx.h"

RC LsmTableEngine::insert_record(Record &record)
{
  RC rc = RC::SUCCESS;
  // TODO: set auto increment id, and keep durability.
  // TODO: support set primary key as a part of lsm_key.
  bytes lsm_key;
  Codec::encode(table_->table_id(), inc_id_.fetch_add(1), lsm_key);
  rc = lsm_->put(string_view((char *)lsm_key.data(), lsm_key.size()), string_view(record.data(), record.len()));
  return rc;
}

RC LsmTableEngine::update_record_with_trx(const Record &old_record, const Record &new_record, Trx *trx)
{
  LsmMvccTrx *lsm_trx = static_cast<LsmMvccTrx *>(trx);
  ObLsmTransaction *ob_trx = lsm_trx->get_trx();
  if (ob_trx == nullptr) {
    LOG_WARN("no transaction context for lsm update");
    return RC::INTERNAL;
  }

  // LSM 引擎中，对于 update 操作，通过事务插入一条新记录
  // 使用与 insert_record 相同的方式生成新 key
  bytes lsm_key;
  Codec::encode(table_->table_id(), inc_id_.fetch_add(1), lsm_key);

  RC rc = ob_trx->put(string_view((char *)lsm_key.data(), lsm_key.size()),
                      string_view(new_record.data(), new_record.len()));
  if (rc != RC::SUCCESS) {
    LOG_WARN("failed to update record via lsm transaction. rc=%s", strrc(rc));
    return rc;
  }

  return RC::SUCCESS;
}

RC LsmTableEngine::get_record_scanner(RecordScanner *&scanner, Trx *trx, ReadWriteMode mode)
{
  scanner = new LsmRecordScanner(table_, db_->lsm(), trx);
  RC rc = scanner->open_scan();
  if (rc != RC::SUCCESS) {
    LOG_ERROR("failed to open scanner. rc=%s", strrc(rc));
  }
  return rc;
}

RC LsmTableEngine::open()
{
  return RC::UNIMPLEMENTED;
}