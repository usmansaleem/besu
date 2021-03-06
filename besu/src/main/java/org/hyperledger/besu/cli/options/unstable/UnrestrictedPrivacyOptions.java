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
package org.hyperledger.besu.cli.options.unstable;

import static picocli.CommandLine.Option;

public class UnrestrictedPrivacyOptions {

  public static UnrestrictedPrivacyOptions create() {
    return new UnrestrictedPrivacyOptions();
  }

  @Option(
      names = "--Xprivacy-unrestricted-enabled",
      description =
          "Enable unrestricted (stores payload onchain) privacy (default: ${DEFAULT-VALUE})",
      hidden = true)
  private final Boolean isUnrestrictedPrivacyEnabled = false;

  public boolean isUnrestrictedPrivacyEnabled() {
    return isUnrestrictedPrivacyEnabled;
  }
}
