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

package com.chaiweijian.groupwallet.userservice;

import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.TestComponent;

import java.util.regex.Pattern;

@TestComponent
public class GroupInvitationNameMatcherTests {
    @Test
    public void testGroupInvitationName() {
        var pattern = Pattern.compile("(users/[a-z0-9-]+)/groupInvitations/[a-z0-9-]+");
        var matcher = pattern.matcher("users/user-id-1/groupInvitations/group-invitation-1");
        if (matcher.find()) {
            assert matcher.group(1).equals("users/user-id-1");
        } else {
            assert false;
        }
    }
}
