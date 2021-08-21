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

import com.chaiweijian.groupwallet.userservice.interfaces.SimpleValidator;
import com.chaiweijian.groupwallet.userservice.v1.User;
import com.google.rpc.BadRequest;
import lombok.Data;

@Data
public class SimpleUserValidator implements SimpleValidator {
    private final User user;
    private final BadRequest badRequest;
    private final boolean failed;

    private SimpleUserValidator(User user, BadRequest badRequest, boolean failed) {
        this.user = user;
        this.badRequest = badRequest;
        this.failed = failed;
    }

    public static SimpleUserValidator validate(User user) {
        return validate(BadRequest.newBuilder(), user);
    }

    public static SimpleUserValidator validate(BadRequest.Builder builder, User user) {
        validateDisplayName(builder, user);
        validateEmail(builder, user);
        validateUid(builder, user);

        return new SimpleUserValidator(user, builder.build(), builder.getFieldViolationsCount() > 0);
    }

    private static void validateDisplayName(BadRequest.Builder badRequestBuilder, User user) {
        var displayName = user.getDisplayName();

        if (displayName.length() == 0) {
            badRequestBuilder.addFieldViolations(BadRequest.FieldViolation.newBuilder().setField("user.displayName").setDescription("User display name must not be empty."));
        } else if (displayName.length() < 4) {
            badRequestBuilder.addFieldViolations(BadRequest.FieldViolation.newBuilder().setField("user.displayName").setDescription("User display name must has minimum 4 characters."));
        } else if (displayName.length() > 120) {
            badRequestBuilder.addFieldViolations(BadRequest.FieldViolation.newBuilder().setField("user.displayName").setDescription("User display name must has maximum 120 characters."));
        }
    }

    private static void validateEmail(BadRequest.Builder badRequestBuilder, User user) {
        var email = user.getEmail();

        if (email.length() == 0) {
            badRequestBuilder.addFieldViolations(BadRequest.FieldViolation.newBuilder().setField("user.email").setDescription("User email must not be empty."));
        }
    }

    private static void validateUid(BadRequest.Builder badRequestBuilder, User user) {
        var uid = user.getUid();

        if (uid.length() == 0) {
            badRequestBuilder.addFieldViolations(BadRequest.FieldViolation.newBuilder().setField("user.uid").setDescription("User uid must not be empty."));
        }
    }
}
