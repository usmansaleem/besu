/*
 * Copyright contributors to Hyperledger Besu.
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
package org.hyperledger.besu.ethereum.core.encoding;

import static org.assertj.core.api.Assertions.assertThat;

import org.hyperledger.besu.datatypes.BLSPublicKey;
import org.hyperledger.besu.datatypes.BLSSignature;
import org.hyperledger.besu.datatypes.GWei;
import org.hyperledger.besu.ethereum.core.DepositRequest;

import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.bytes.Bytes32;
import org.apache.tuweni.units.bigints.UInt64;
import org.junit.jupiter.api.Test;

class DepositRequestEncoderTest {
  private final String expectedDepositEncodedBytes =
      "f8bbb0b10a4a15bf67b328c9b101d09e5c6ee6672978fdad9ef0d9e2ceffaee99223555d8601f0cb3bcc4ce1af9864779a416ea00017a7fcf06faf493d30bbe2632ea7c2383cd86825e12797165de7aa35589483850773594000b860a889db8300194050a2636c92a95bc7160515867614b7971a9500cdb62f9c0890217d2901c3241f86fac029428fc106930606154bd9e406d7588934a5f15b837180b17194d6e44bd6de23e43b163dfe12e369dcc75a3852cd997963f158217eb501";

  final DepositRequest depositRequest =
      new DepositRequest(
          BLSPublicKey.fromHexString(
              "0xb10a4a15bf67b328c9b101d09e5c6ee6672978fdad9ef0d9e2ceffaee99223555d8601f0cb3bcc4ce1af9864779a416e"),
          Bytes32.fromHexString(
              "0x0017a7fcf06faf493d30bbe2632ea7c2383cd86825e12797165de7aa35589483"),
          GWei.of(32000000000L),
          BLSSignature.fromHexString(
              "0xa889db8300194050a2636c92a95bc7160515867614b7971a9500cdb62f9c0890217d2901c3241f86fac029428fc106930606154bd9e406d7588934a5f15b837180b17194d6e44bd6de23e43b163dfe12e369dcc75a3852cd997963f158217eb5"),
          UInt64.ONE);

  @Test
  void shouldEncodeDeposit() {
    final Bytes encoded = DepositRequestEncoder.encodeOpaqueBytes(depositRequest);
    assertThat(encoded).isEqualTo(Bytes.fromHexString(expectedDepositEncodedBytes));
  }

  @Test
  void shouldEncodeDepositRequest() {
    final Bytes encoded = RequestEncoder.encodeOpaqueBytes(depositRequest);
    // Request encoding is Request = RequestType ++ RequestData
    assertThat(encoded)
        .isEqualTo(
            Bytes.fromHexString(
                String.format(
                    "0x%02X%s",
                    depositRequest.getType().getSerializedType(), expectedDepositEncodedBytes)));
  }
}
