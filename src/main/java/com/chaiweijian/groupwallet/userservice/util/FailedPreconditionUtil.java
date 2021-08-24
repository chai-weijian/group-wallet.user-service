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

import com.google.protobuf.Any;
import com.google.rpc.Code;
import com.google.rpc.PreconditionFailure;
import com.google.rpc.Status;

public class FailedPreconditionUtil {
    public static Status packStatus(PreconditionFailure preconditionFailure) {
        return Status.newBuilder()
                .setCode(Code.FAILED_PRECONDITION_VALUE)
                .addDetails(Any.pack(preconditionFailure))
                .build();
    }
}
