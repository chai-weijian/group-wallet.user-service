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

// A simple wrapper for validation.
// This class provide both fail and pass API as this is
// more convenient. When calling setFail and setPass
// for more than one time, the last one win.
public class ValidationResult<T> {

    public ValidationResult(T item) {
        this.item = item;
    }

    private final T item;

    public T getItem() {
        return item;
    }

    private boolean failed;

    public ValidationResult<T> setFail(boolean isFail) {
        this.failed = isFail;
        return this;
    }

    public ValidationResult<T> setPass(boolean isPass) {
        this.failed = !isPass;
        return this;
    }

    public boolean isFailed() {
        return failed;
    }

    public boolean isPassed() {
        return !failed;
    }
}