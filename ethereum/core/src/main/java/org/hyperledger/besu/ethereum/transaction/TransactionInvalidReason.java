/*
 * Copyright ConsenSys AG.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package org.hyperledger.besu.ethereum.transaction;

public enum TransactionInvalidReason {
  WRONG_CHAIN_ID,
  REPLAY_PROTECTED_SIGNATURES_NOT_SUPPORTED,
  REPLAY_PROTECTED_SIGNATURE_REQUIRED,
  INVALID_SIGNATURE,
  UPFRONT_COST_EXCEEDS_BALANCE,
  UPFRONT_COST_EXCEEDS_UINT256,
  NONCE_TOO_LOW,
  NONCE_TOO_HIGH,
  NONCE_OVERFLOW,
  INTRINSIC_GAS_EXCEEDS_GAS_LIMIT,
  EXCEEDS_BLOCK_GAS_LIMIT,
  EXCEEDS_TRANSACTION_GAS_LIMIT,
  TX_SENDER_NOT_AUTHORIZED,
  CHAIN_HEAD_NOT_AVAILABLE,
  CHAIN_HEAD_WORLD_STATE_NOT_AVAILABLE,
  BLOCK_NOT_FOUND,
  EXCEEDS_PER_TRANSACTION_GAS_LIMIT,
  INVALID_TRANSACTION_FORMAT,
  TRANSACTION_PRICE_TOO_LOW,
  TRANSACTION_ALREADY_KNOWN,
  TRANSACTION_REPLACEMENT_UNDERPRICED,
  MAX_PRIORITY_FEE_PER_GAS_EXCEEDS_MAX_FEE_PER_GAS,
  INITCODE_TOO_LARGE,
  NONCE_TOO_FAR_IN_FUTURE_FOR_SENDER,
  TOTAL_BLOB_GAS_TOO_HIGH,
  GAS_PRICE_TOO_LOW,
  GAS_PRICE_BELOW_CURRENT_BASE_FEE,
  BLOB_GAS_PRICE_BELOW_CURRENT_BLOB_BASE_FEE,
  MAX_FEE_PER_GAS_BELOW_CURRENT_BASE_FEE,
  TX_FEECAP_EXCEEDED,
  INTERNAL_ERROR,
  EXECUTION_INTERRUPTED,
  TX_POOL_DISABLED,
  INVALID_BLOBS,
  PLUGIN_TX_VALIDATOR,
  INVALID_BLOB_COUNT,
  PLUGIN_TX_POOL_VALIDATOR,
  EXECUTION_HALTED,
  EOF_CODE_INVALID,
  EMPTY_CODE_DELEGATION
}
