/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#pragma once

#include <folly/io/Cursor.h>
#include <folly/io/IOBuf.h>

namespace celeborn {
class ReadOnlyByteBuffer;
class WriteOnlyByteBuffer;

class ByteBuffer {
 public:
  static std::unique_ptr<WriteOnlyByteBuffer> createWriteOnly(
      size_t initialCapacity,
      bool isBigEndian = true);

  static std::unique_ptr<ReadOnlyByteBuffer> createReadOnly(
      std::unique_ptr<folly::IOBuf>&& data,
      bool isBigEndian = true);

  static std::unique_ptr<ReadOnlyByteBuffer> toReadOnly(
      std::unique_ptr<ByteBuffer>&& buffer);

  static std::unique_ptr<ReadOnlyByteBuffer> concat(
      const ReadOnlyByteBuffer& left,
      const ReadOnlyByteBuffer& right);

 protected:
  ByteBuffer(std::unique_ptr<folly::IOBuf> data, bool isBigEndian)
      : data_(std::move(data)), isBigEndian_(isBigEndian) {
    assert(data_);
  }

  std::unique_ptr<folly::IOBuf> data_;
  bool isBigEndian_;

 private:
  static std::unique_ptr<folly::IOBuf> trimBuffer(
      const ReadOnlyByteBuffer& buffer);
};

class ReadOnlyByteBuffer : public ByteBuffer {
 public:
  friend class ByteBuffer;

  static std::unique_ptr<ReadOnlyByteBuffer> createEmptyBuffer() {
    return createReadOnly(folly::IOBuf::create(0));
  }

  ReadOnlyByteBuffer(const ReadOnlyByteBuffer& other)
      : ByteBuffer(other.data_->clone(), other.isBigEndian_),
        cursor_(std::make_unique<folly::io::Cursor>(data_.get())) {
    cursor_->skip(other.cursor_->getCurrentPosition());
    assert(other.remainingSize() == remainingSize());
  }

  ReadOnlyByteBuffer(std::unique_ptr<folly::IOBuf>&& data, bool isBigEndian)
      : ByteBuffer(std::move(data), isBigEndian),
        cursor_(std::make_unique<folly::io::Cursor>(data_.get())) {}

  std::unique_ptr<ReadOnlyByteBuffer> clone() {
    return std::make_unique<ReadOnlyByteBuffer>(*this);
  }

  template <typename T>
  T read() {
    if (isBigEndian_) {
      return readBE<T>();
    }
    return readLE<T>();
  }

  template <typename T>
  T readBE() {
    return cursor_->readBE<T>();
  }

  template <typename T>
  T readLE() {
    return cursor_->readLE<T>();
  }

  void skip(size_t len) const {
    cursor_->skip(len);
  }

  void retreat(size_t len) const {
    cursor_->retreat(len);
  }

  size_t size() const {
    return data_->computeChainDataLength();
  }

  // TODO: this interface is called rapidly. maybe need optimization.
  size_t remainingSize() const {
    return cursor_->totalLength();
  }

  std::string readToString() const {
    return readToString(remainingSize());
  }

  std::string readToString(size_t len) const {
    return cursor_->readFixedString(len);
  }

  size_t readToBuffer(void* buf, size_t len) const {
    return cursor_->pullAtMost(buf, len);
  }

  std::unique_ptr<folly::IOBuf> getData() const {
    return data_->clone();
  }

 private:
  std::unique_ptr<folly::io::Cursor> cursor_;
};

class WriteOnlyByteBuffer : public ByteBuffer {
 public:
  friend class ByteBuffer;

  // TODO: currently the appender is not allowed to grow.
  WriteOnlyByteBuffer(size_t initialCapacity, bool isBigEndian)
      : ByteBuffer(folly::IOBuf::createCombined(initialCapacity), isBigEndian),
        appender_(std::make_unique<folly::io::Appender>(data_.get(), 0)) {}

  WriteOnlyByteBuffer(std::unique_ptr<folly::IOBuf> data, bool isBigEndian)
      : ByteBuffer(std::move(data), isBigEndian),
        appender_(std::make_unique<folly::io::Appender>(data_.get(), 0)) {}

  template <class T>
  void write(T value) {
    if (isBigEndian_) {
      writeBE(value);
      return;
    }
    writeLE(value);
  }

  template <class T>
  void writeBE(T value) {
    appender_->writeBE(value);
  }

  template <class T>
  void writeLE(T value) {
    appender_->writeLE(value);
  }

  void writeFromString(const std::string& data) const {
    auto ptr = data.c_str();
    appender_->push(reinterpret_cast<const uint8_t*>(ptr), data.size());
  }

  size_t size() const {
    return data_->computeChainDataLength();
  }

 private:
  std::unique_ptr<folly::io::Appender> appender_;
};
} // namespace celeborn
