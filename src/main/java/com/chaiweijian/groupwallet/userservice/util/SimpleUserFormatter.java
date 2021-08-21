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

import com.chaiweijian.groupwallet.userservice.v1.User;

public class SimpleUserFormatter {
    public static User format(User user) {
        return user.toBuilder()
                .setDisplayName(user.getDisplayName().trim())
                .setEmail(user.getEmail().trim())
                .build();
    }
}
