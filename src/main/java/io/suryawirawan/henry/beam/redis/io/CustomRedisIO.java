/*
 * Copyright 2018 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.suryawirawan.henry.beam.redis.io;

import static com.google.common.base.Preconditions.checkArgument;

import com.google.auto.value.AutoValue;
import javax.annotation.Nullable;
import org.apache.beam.sdk.io.redis.RedisConnectionConfiguration;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PDone;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.Pipeline;

public class CustomRedisIO {

  private CustomRedisIO() {}

  /** Write data to a Redis server. */
  public static CustomRedisIO.Write write() {
    return new AutoValue_CustomRedisIO_Write.Builder()
        .setConnectionConfiguration(RedisConnectionConfiguration.create())
        .setMethod(Write.Method.APPEND)
        .build();
  }

  /** A {@link PTransform} to write to a Redis server. */
  @AutoValue
  public abstract static class Write extends PTransform<PCollection<KV<String, String>>, PDone> {

    /** Determines the method used to insert data in Redis. */
    public enum Method {

      /**
       * Use APPEND command. If key already exists and is a string, this command appends the value
       * at the end of the string.
       */
      APPEND,

      /** Use SET command. If key already holds a value, it is overwritten. */
      SET,

      /**
       * Use LPUSH command. Insert value at the head of the list stored at key. If key does not
       * exist, it is created as empty list before performing the push operations. When key holds a
       * value that is not a list, an error is returned.
       */
      LPUSH,

      /**
       * Use RPUSH command. Insert value at the tail of the list stored at key. If key does not
       * exist, it is created as empty list before performing the push operations. When key holds a
       * value that is not a list, an error is returned.
       */
      RPUSH
    }

    @Nullable
    abstract RedisConnectionConfiguration connectionConfiguration();

    @Nullable
    abstract Method method();

    @Nullable
    abstract Long expireTime();

    abstract CustomRedisIO.Write.Builder builder();

    @AutoValue.Builder
    abstract static class Builder {

      abstract CustomRedisIO.Write.Builder setConnectionConfiguration(
          RedisConnectionConfiguration connectionConfiguration);

      abstract CustomRedisIO.Write.Builder setMethod(Method method);

      abstract CustomRedisIO.Write.Builder setExpireTime(Long expireTimeMillis);

      abstract CustomRedisIO.Write build();
    }

    public CustomRedisIO.Write withEndpoint(String host, int port) {
      checkArgument(host != null, "host can not be null");
      checkArgument(port > 0, "port can not be negative or 0");
      return builder()
          .setConnectionConfiguration(connectionConfiguration().withHost(host).withPort(port))
          .build();
    }

    public CustomRedisIO.Write withAuth(String auth) {
      checkArgument(auth != null, "auth can not be null");
      return builder().setConnectionConfiguration(connectionConfiguration().withAuth(auth)).build();
    }

    public CustomRedisIO.Write withTimeout(int timeout) {
      checkArgument(timeout >= 0, "timeout can not be negative");
      return builder()
          .setConnectionConfiguration(connectionConfiguration().withTimeout(timeout))
          .build();
    }

    public CustomRedisIO.Write withConnectionConfiguration(
        RedisConnectionConfiguration connection) {
      checkArgument(connection != null, "connection can not be null");
      return builder().setConnectionConfiguration(connection).build();
    }

    public CustomRedisIO.Write withMethod(Method method) {
      checkArgument(method != null, "method can not be null");
      return builder().setMethod(method).build();
    }

    public CustomRedisIO.Write withExpireTime(Long expireTimeMillis) {
      checkArgument(expireTimeMillis != null, "expireTimeMillis can not be null");
      checkArgument(expireTimeMillis > 0, "expireTimeMillis can not be negative or 0");
      return builder().setExpireTime(expireTimeMillis).build();
    }

    @Override
    public PDone expand(PCollection<KV<String, String>> input) {
      checkArgument(connectionConfiguration() != null, "withConnectionConfiguration() is required");

      input.apply(ParDo.of(new CustomRedisIO.Write.WriteFn(this)));

      return PDone.in(input.getPipeline());
    }

    private static class WriteFn extends DoFn<KV<String, String>, Void> {

      private static final int DEFAULT_BATCH_SIZE = 1000;

      private final CustomRedisIO.Write spec;

      private transient Jedis jedis;
      private transient Pipeline pipeline;

      private int batchCount;

      public WriteFn(CustomRedisIO.Write spec) {
        this.spec = spec;
      }

      @Setup
      public void setup() {
        jedis = spec.connectionConfiguration().connect();
      }

      @StartBundle
      public void startBundle() {
        pipeline = jedis.pipelined();
        pipeline.multi();
        batchCount = 0;
      }

      @ProcessElement
      public void processElement(ProcessContext processContext) {
        KV<String, String> record = processContext.element();

        writeRecord(record);

        batchCount++;

        if (batchCount >= DEFAULT_BATCH_SIZE) {
          pipeline.exec();
          batchCount = 0;
        }
      }

      private void writeRecord(KV<String, String> record) {
        Method method = spec.method();
        Long expireTime = spec.expireTime();

        if (Method.APPEND == method) {
          writeUsingAppendCommand(record);
        } else if (Method.SET == method) {
          writeUsingSetCommand(record, expireTime);
        } else if (Method.LPUSH == method || Method.RPUSH == method) {
          writeUsingListCommand(record, method, expireTime);
        }
      }

      private void writeUsingAppendCommand(KV<String, String> record) {
        pipeline.append(record.getKey(), record.getValue());
      }

      private void writeUsingSetCommand(KV<String, String> record, Long expireTime) {
        if (expireTime != null) {
          pipeline.psetex(record.getKey(), expireTime, record.getValue());
        } else {
          pipeline.set(record.getKey(), record.getValue());
        }
      }

      private void writeUsingListCommand(
          KV<String, String> record, Method method, Long expireTime) {

        if (Method.LPUSH == method) {
          pipeline.lpush(record.getKey(), record.getValue());
        } else if (Method.RPUSH == method) {
          pipeline.rpush(record.getKey(), record.getValue());
        }

        if (expireTime != null) {
          pipeline.pexpire(record.getKey(), expireTime);
        }
      }

      @FinishBundle
      public void finishBundle() {
        pipeline.exec();
        batchCount = 0;
      }

      @Teardown
      public void teardown() {
        jedis.close();
      }
    }
  }
}
