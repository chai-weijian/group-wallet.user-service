// Copyright 2021 Chai Wei Jian
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package com.chaiweijian.groupwallet.userservice.util;

import com.google.common.hash.Hashing;

import java.nio.charset.StandardCharsets;

public class EtagUtil {
    /**
     * Returns an etag based on provided etagSecret and aggregateVersion.
     *
     * @param   etagSecret  Fixed string to append to aggregate version before calculating etag. This is to prevent client from guessing the next etag.
     * @param   resourceName  The resource to generate etag
     * @param   aggregateVersion    The aggregate version of object to calculate etag
     * @return  an opaque string that is unique for each aggregate version
     */
    public static String calculateEtag(String etagSecret, String resourceName, Integer aggregateVersion) {
        //noinspection UnstableApiUsage
        var calculated = Hashing.sha256().hashString(String.format("%s-%s-%d", etagSecret, resourceName, aggregateVersion), StandardCharsets.UTF_8).toString();

        // ETag values should include quotes as described in [RFC 7232](https://datatracker.ietf.org/doc/html/rfc7232#section-2.3).
        // For example, a valid etag is "foo", not foo.
        return String.format("\"%s\"", calculated);
    }
}
