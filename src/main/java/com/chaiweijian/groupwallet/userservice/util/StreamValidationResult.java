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

import com.google.rpc.Status;
import lombok.Data;
import org.apache.kafka.streams.kstream.KStream;

// A wrapper for a validation of stream. This assumed the stream is not repartitioned
// and key type is not changed during the validation.
// The validation should return 2 branch of stream, one passed and one failed stream,
// the failed stream should map to Status.
@Data
public class StreamValidationResult<K, T> {
    // this stream will emit when the validation is passed, return the original
    // value
    private final KStream<K, T> passedStream;

    // this stream will emit when the validation is failed, return the original
    // value
    private final KStream<K, T> failedStream;

    // this stream will emit when the validation is failed, return Status containing
    // details on why the validation failed.
    private final KStream<K, Status> statusStream;
}
