/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

#ifdef HAVE_CONFIG_H
#include "config.h"
#endif

#include <sys/types.h>
#if defined( WIN32 ) || defined( _WIN64 )
typedef int  int32_t;
typedef signed char int8_t;
typedef unsigned char   uint8_t;
typedef unsigned short  uint16_t;
typedef long long  int64_t;
typedef unsigned   uint32_t;
typedef short  int16_t;
typedef unsigned long long   uint64_t;
#else
#include <arpa/inet.h>
#endif
#include <stdexcept>

#ifndef bswap_64
#define	bswap_64(x)     (((uint64_t)(x) << 56) | \
                        (((uint64_t)(x) << 40) & 0xff000000000000ULL) | \
                        (((uint64_t)(x) << 24) & 0xff0000000000ULL) | \
                        (((uint64_t)(x) << 8)  & 0xff00000000ULL) | \
                        (((uint64_t)(x) >> 8)  & 0xff000000ULL) | \
                        (((uint64_t)(x) >> 24) & 0xff0000ULL) | \
                        (((uint64_t)(x) >> 40) & 0xff00ULL) | \
                        ((uint64_t)(x)  >> 56))
#endif

#if __BYTE_ORDER == __LITTLE_ENDIAN
#define htonll(x) bswap_64(x)
#define ntohll(x) bswap_64(x)
#elif __BYTE_ORDER == __BIG_ENDIAN
#define htonll(x) x
#define ntohll(x) x
#else
#error Unknown __BYTE_ORDER
#endif

enum TType {
  T_STOP       = 0,
  T_VOID       = 1,
  T_BOOL       = 2,
  T_BYTE       = 3,
  T_I08        = 3,
  T_I16        = 6,
  T_I32        = 8,
  T_U64        = 9,
  T_I64        = 10,
  T_DOUBLE     = 4,
  T_STRING     = 11,
  T_UTF7       = 11,
  T_STRUCT     = 12,
  T_MAP        = 13,
  T_SET        = 14,
  T_LIST       = 15,
  T_UTF8       = 16,
  T_UTF16      = 17
};

const int32_t VERSION_MASK = 0xffff0000;
const int32_t VERSION_1 = 0x80010000;
const int8_t T_CALL = 1;
const int8_t T_REPLY = 2;
const int8_t T_EXCEPTION = 3;
// tprotocolexception
const int INVALID_DATA = 1;
const int BAD_VERSION = 4;

#include "php.h"
#include "zend_interfaces.h"
#include "zend_exceptions.h"
#include "php_thrift_protocol.h"

static zend_function_entry thrift_protocol_functions[] = {
  PHP_FE(thrift_protocol_write_binary, NULL)
  PHP_FE(thrift_protocol_read_binary, NULL)
  {NULL, NULL, NULL}
} ;

zend_module_entry thrift_protocol_module_entry = {
  STANDARD_MODULE_HEADER,
  "thrift_protocol",
  thrift_protocol_functions,
  NULL,
  NULL,
  NULL,
  NULL,
  NULL,
  "1.0",
  STANDARD_MODULE_PROPERTIES
};

#ifdef COMPILE_DL_THRIFT_PROTOCOL
ZEND_GET_MODULE(thrift_protocol)
#endif

class PHPExceptionWrapper : public std::exception {
public:
  PHPExceptionWrapper(zval* _ex) throw() : ex(_ex) {
    snprintf(_what, 40, "PHP exception zval=%p", ex);
  }
  const char* what() const throw() { return _what; }
  ~PHPExceptionWrapper() throw() {}
  operator zval*() const throw() { return const_cast<zval*>(ex); } // Zend API doesn't do 'const'...
protected:
  zval* ex;
  char _what[40];
} ;

class PHPTransport {
public:
  zval* protocol() { return &p; }
  zval* transport() { return &t; }
protected:
  PHPTransport() {}

  void construct_with_zval(zval* _p, size_t _buffer_size) {
    buffer = reinterpret_cast<char*>(emalloc(_buffer_size));
    buffer_ptr = buffer;
    buffer_used = 0;
    buffer_size = _buffer_size;
    ZVAL_DUP(&p, _p);

    // Get the transport for the passed protocol
    zval gettransport;
    ZVAL_STRING(&gettransport, "getTransport");
    ZVAL_NULL(&t);
    call_user_function(EG(function_table), &p, &gettransport, &t, 0, NULL);
  }
  ~PHPTransport() {
    efree(buffer);
    zval_ptr_dtor(&t);
  }

  char* buffer;
  char* buffer_ptr;
  size_t buffer_used;
  size_t buffer_size;

  zval p;
  zval t;
};


class PHPOutputTransport : public PHPTransport {
public:
  PHPOutputTransport(zval* _p, size_t _buffer_size = 8192) {
    construct_with_zval(_p, _buffer_size);
  }

  ~PHPOutputTransport() {
    //flush();
  }

  void write(const char* data, size_t len) {
    if ((len + buffer_used) > buffer_size) {
      internalFlush();
    }
    if (len > buffer_size) {
      directWrite(data, len);
    } else {
      memcpy(buffer_ptr, data, len);
      buffer_used += len;
      buffer_ptr += len;
    }
  }

  void writeI64(int64_t i) {
    i = htonll(i);
    write((const char*)&i, 8);
  }

  void writeU32(uint32_t i) {
    i = htonl(i);
    write((const char*)&i, 4);
  }

  void writeI32(int32_t i) {
    i = htonl(i);
    write((const char*)&i, 4);
  }

  void writeI16(int16_t i) {
    i = htons(i);
    write((const char*)&i, 2);
  }

  void writeI8(int8_t i) {
    write((const char*)&i, 1);
  }

  void writeString(const char* str, size_t len) {
    writeU32(len);
    write(str, len);
  }

  void flush() {
    internalFlush();
    directFlush();
  }

protected:
  void internalFlush() {
     if (buffer_used) {
      directWrite(buffer, buffer_used);
      buffer_ptr = buffer;
      buffer_used = 0;
    }
  }
  void directFlush() {
    zval ret;
    ZVAL_NULL(&ret);
    zval flushfn;
    ZVAL_STRING(&flushfn, "flush");
    TSRMLS_FETCH();
    call_user_function(EG(function_table), &t, &flushfn, &ret, 0, NULL TSRMLS_CC);
    zval_dtor(&ret);
  }
  void directWrite(const char* data, size_t len) {
    zval writefn;
    ZVAL_STRING(&writefn, "write");
    char* newbuf = (char*)emalloc(len + 1);
    memcpy(newbuf, data, len);
    newbuf[len] = '\0';
    zval args[1];
    ZVAL_STRINGL(&args[0], newbuf, len);
    zval ret;
    ZVAL_NULL(&ret);
    call_user_function(EG(function_table), &t, &writefn, &ret, 1, args TSRMLS_CC);
    zval_ptr_dtor(args);
    zval_dtor(&ret);
    if (EG(exception)) {
      // TODO Sean-Der
      //zval* ex = EG(exception);
      //EG(exception) = NULL;
      //throw PHPExceptionWrapper(ex);
    }
  }
};

class PHPInputTransport : public PHPTransport {
public:
  PHPInputTransport(zval* _p, size_t _buffer_size = 8192) {
    construct_with_zval(_p, _buffer_size);
  }

  ~PHPInputTransport() {
    put_back();
  }

  void put_back() {
    if (buffer_used) {
      zval putbackfn;
      ZVAL_STRING(&putbackfn, "putBack");

      char* newbuf = (char*)emalloc(buffer_used + 1);
      memcpy(newbuf, buffer_ptr, buffer_used);
      newbuf[buffer_used] = '\0';

      zval args[1];
      ZVAL_STRINGL(&args[0], newbuf, buffer_used);

      zval ret;
      ZVAL_NULL(&ret);
      call_user_function(EG(function_table), &t, &putbackfn, &ret, 1, args TSRMLS_CC);
      zval_ptr_dtor(args);
      zval_dtor(&ret);
    }
    buffer_used = 0;
    buffer_ptr = buffer;
  }

  void skip(size_t len) {
    while (len) {
      size_t chunk_size = MIN(len, buffer_used);
      if (chunk_size) {
        buffer_ptr = reinterpret_cast<char*>(buffer_ptr) + chunk_size;
        buffer_used -= chunk_size;
        len -= chunk_size;
      }
      if (! len) break;
      refill();
    }
  }

  void readBytes(void* buf, size_t len) {
    while (len) {
      size_t chunk_size = MIN(len, buffer_used);
      if (chunk_size) {
        memcpy(buf, buffer_ptr, chunk_size);
        buffer_ptr = reinterpret_cast<char*>(buffer_ptr) + chunk_size;
        buffer_used -= chunk_size;
        buf = reinterpret_cast<char*>(buf) + chunk_size;
        len -= chunk_size;
      }
      if (! len) break;
      refill();
    }
  }

  int8_t readI8() {
    int8_t c;
    readBytes(&c, 1);
    return c;
  }

  int16_t readI16() {
    int16_t c;
    readBytes(&c, 2);
    return (int16_t)ntohs(c);
  }

  uint32_t readU32() {
    uint32_t c;
    readBytes(&c, 4);
    return (uint32_t)ntohl(c);
  }

  int32_t readI32() {
    int32_t c;
    readBytes(&c, 4);
    return (int32_t)ntohl(c);
  }

protected:
  void refill() {
    assert(buffer_used == 0);
    zval retval;
    ZVAL_NULL(&retval);

    zval args[1];
    ZVAL_LONG(&args[0], buffer_size);

    TSRMLS_FETCH();

    zval funcname;
    ZVAL_STRING(&funcname, "read");

    call_user_function(EG(function_table), &t, &funcname, &retval, 1, args TSRMLS_CC);
    zval_ptr_dtor(args);

    if (EG(exception)) {
      // TODO Sean-Der
      //zval_dtor(&retval);
      //zend_object *ex = EG(exception);
      //EG(exception) = NULL;
      //throw PHPExceptionWrapper(ex);
    }

    buffer_used = Z_STRLEN(retval);
    memcpy(buffer, Z_STRVAL(retval), buffer_used);
    zval_dtor(&retval);

    buffer_ptr = buffer;
  }

};

void binary_deserialize_spec(zval* zthis, PHPInputTransport& transport, HashTable* spec);
void binary_serialize_spec(zval* zthis, PHPOutputTransport& transport, HashTable* spec);
void binary_serialize(int8_t thrift_typeID, PHPOutputTransport& transport, zval** value, HashTable* fieldspec);
void skip_element(long thrift_typeID, PHPInputTransport& transport);
void protocol_writeMessageBegin(zval *transport, const char* method_name, int32_t msgtype, int32_t seqID);


// Create a PHP object given a typename and call the ctor, optionally passing up to 2 arguments
void createObject(char* obj_typename, zval* return_value, int nargs = 0, zval* arg1 = NULL, zval* arg2 = NULL) {
  zend_string *class_name = zend_string_init(obj_typename, strlen(obj_typename), 0);
  zend_class_entry* ce = zend_fetch_class(class_name, ZEND_FETCH_CLASS_DEFAULT);

  zend_string_release(class_name);
  if (! ce) {
    php_error_docref(NULL TSRMLS_CC, E_ERROR, "Class %s does not exist", obj_typename);
    RETURN_NULL();
  }

  object_and_properties_init(return_value, ce, NULL);
  zval ctor_rv;
  zend_call_method(return_value, ce, &ce->constructor, NULL, 0, &ctor_rv, nargs, arg1, arg2 TSRMLS_CC);
  zval_ptr_dtor(&ctor_rv);
}

void throw_tprotocolexception(char* what, long errorcode) {
  TSRMLS_FETCH();

  zval zwhat, zerrorcode;

  ZVAL_STRING(&zwhat, what);
  ZVAL_LONG(&zerrorcode, errorcode);

  zval ex;
  createObject((char *) "\\Thrift\\Exception\\TProtocolException", &ex, 2, &zwhat, &zerrorcode);
  zval_ptr_dtor(&zwhat);
  zval_ptr_dtor(&zerrorcode);
  throw PHPExceptionWrapper(&ex);
}

// Sets EG(exception), call this and then RETURN_NULL();
void throw_zend_exception_from_std_exception(const std::exception& ex TSRMLS_DC) {
  zend_throw_exception(zend_exception_get_default(TSRMLS_C), const_cast<char*>(ex.what()), 0 TSRMLS_CC);
}


void binary_deserialize(int8_t thrift_typeID, PHPInputTransport& transport, zval* return_value, HashTable* fieldspec) {
  zval *val_ptr;
  ZVAL_NULL(return_value);

  switch (thrift_typeID) {
    case T_STOP:
    case T_VOID:
      RETURN_NULL();
      return;
    case T_STRUCT: {
      if ((val_ptr = zend_hash_str_find(fieldspec, ZEND_STRL("class"))) == NULL) {
        throw_tprotocolexception((char *)"no class type in spec", INVALID_DATA);
        skip_element(T_STRUCT, transport);
        RETURN_NULL();
      }
      char* structType = Z_STRVAL_P(val_ptr);
      createObject(structType, return_value);
      if (Z_TYPE_P(return_value) == IS_NULL) {
        // unable to create class entry
        skip_element(T_STRUCT, transport);
        RETURN_NULL();
      }
      zval* spec = zend_read_static_property(Z_OBJCE_P(return_value TSRMLS_CC), "_TSPEC", 6, false TSRMLS_CC);
      if (Z_TYPE_P(spec) != IS_ARRAY) {
        char errbuf[128];
        snprintf(errbuf, 128, "spec for %s is wrong type: %d\n", structType, Z_TYPE_P(spec));
        throw_tprotocolexception(errbuf, INVALID_DATA);
        RETURN_NULL();
      }
      binary_deserialize_spec(return_value, transport, Z_ARRVAL_P(spec));
      return;
    } break;
    case T_BOOL: {
      uint8_t c;
      transport.readBytes(&c, 1);
      RETURN_BOOL(c != 0);
    }
  //case T_I08: // same numeric value as T_BYTE
    case T_BYTE: {
      uint8_t c;
      transport.readBytes(&c, 1);
      RETURN_LONG((int8_t)c);
    }
    case T_I16: {
      uint16_t c;
      transport.readBytes(&c, 2);
      RETURN_LONG((int16_t)ntohs(c));
    }
    case T_I32: {
      uint32_t c;
      transport.readBytes(&c, 4);
      RETURN_LONG((int32_t)ntohl(c));
    }
    case T_U64:
    case T_I64: {
      uint64_t c;
      transport.readBytes(&c, 8);
      RETURN_LONG((int64_t)ntohll(c));
    }
    case T_DOUBLE: {
      union {
        uint64_t c;
        double d;
      } a;
      transport.readBytes(&(a.c), 8);
      a.c = ntohll(a.c);
      RETURN_DOUBLE(a.d);
    }
    //case T_UTF7: // aliases T_STRING
    case T_UTF8:
    case T_UTF16:
    case T_STRING: {
      uint32_t size = transport.readU32();
      if (size) {
        char* strbuf = (char*) emalloc(size + 1);
        transport.readBytes(strbuf, size);
        strbuf[size] = '\0';
        ZVAL_STRINGL(return_value, strbuf, size);
      } else {
        ZVAL_EMPTY_STRING(return_value);
      }
      return;
    }
    case T_MAP: { // array of key -> value
      uint8_t types[2];
      transport.readBytes(types, 2);
      uint32_t size = transport.readU32();
      array_init(return_value);

      val_ptr = zend_hash_str_find(fieldspec, ZEND_STRL("key"));
      HashTable* keyspec = Z_ARRVAL_P(val_ptr);
      val_ptr = zend_hash_str_find(fieldspec, ZEND_STRL("val"));
      HashTable* valspec = Z_ARRVAL_P(val_ptr);

      for (uint32_t s = 0; s < size; ++s) {
        zval value, key;
        ZVAL_UNDEF(&value);
        ZVAL_UNDEF(&key);

        binary_deserialize(types[0], transport, &key, keyspec);
        binary_deserialize(types[1], transport, &value, valspec);
        if (Z_TYPE(key) == IS_LONG) {
          zend_hash_index_update(HASH_OF(return_value), Z_LVAL(key), &value);
        }
        else {
          if (Z_TYPE(key) != IS_STRING) convert_to_string(&key);
          zend_hash_str_update(HASH_OF(return_value), Z_STRVAL(key), Z_STRLEN(key) + 1, &value);
        }
        zval_ptr_dtor(&key);
      }
      return; // return_value already populated
    }
    case T_LIST: { // array with autogenerated numeric keys
      int8_t type = transport.readI8();
      uint32_t size = transport.readU32();
      val_ptr = zend_hash_str_find(fieldspec, ZEND_STRL("elem"));
      HashTable* elemspec = Z_ARRVAL_P(val_ptr);

      array_init(return_value);
      for (uint32_t s = 0; s < size; ++s) {
        zval value;
        ZVAL_UNDEF(&value);
        binary_deserialize(type, transport, &value, elemspec);
        zend_hash_next_index_insert(HASH_OF(return_value), &value);
      }
      return;
    }
    case T_SET: { // array of key -> TRUE
      uint8_t type;
      uint32_t size;
      transport.readBytes(&type, 1);
      transport.readBytes(&size, 4);
      size = ntohl(size);
      val_ptr = zend_hash_str_find(fieldspec, ZEND_STRL("elem"));
      HashTable* elemspec = Z_ARRVAL_P(val_ptr);

      array_init(return_value);

      for (uint32_t s = 0; s < size; ++s) {
        zval key, value;
        ZVAL_TRUE(&value);
        ZVAL_UNDEF(&key);

        binary_deserialize(type, transport, &key, elemspec);

        if (Z_TYPE(key) == IS_LONG) {
          zend_hash_index_update(HASH_OF(return_value), Z_LVAL(key), &value);
        }
        else {
          if (Z_TYPE(key) != IS_STRING) convert_to_string(&key);
          zend_hash_str_update(HASH_OF(return_value), Z_STRVAL(key), Z_STRLEN(key) + 1, &value);
        }
        zval_ptr_dtor(&key);
      }
      return;
    }
  };

  char errbuf[128];
  sprintf(errbuf, "Unknown thrift typeID %d", thrift_typeID);
  throw_tprotocolexception(errbuf, INVALID_DATA);
}

void skip_element(long thrift_typeID, PHPInputTransport& transport) {
  switch (thrift_typeID) {
    case T_STOP:
    case T_VOID:
      return;
    case T_STRUCT:
      while (true) {
        int8_t ttype = transport.readI8(); // get field type
        if (ttype == T_STOP) break;
        transport.skip(2); // skip field number, I16
        skip_element(ttype, transport); // skip field payload
      }
      return;
    case T_BOOL:
    case T_BYTE:
      transport.skip(1);
      return;
    case T_I16:
      transport.skip(2);
      return;
    case T_I32:
      transport.skip(4);
      return;
    case T_U64:
    case T_I64:
    case T_DOUBLE:
      transport.skip(8);
      return;
    //case T_UTF7: // aliases T_STRING
    case T_UTF8:
    case T_UTF16:
    case T_STRING: {
      uint32_t len = transport.readU32();
      transport.skip(len);
      } return;
    case T_MAP: {
      int8_t keytype = transport.readI8();
      int8_t valtype = transport.readI8();
      uint32_t size = transport.readU32();
      for (uint32_t i = 0; i < size; ++i) {
        skip_element(keytype, transport);
        skip_element(valtype, transport);
      }
    } return;
    case T_LIST:
    case T_SET: {
      int8_t valtype = transport.readI8();
      uint32_t size = transport.readU32();
      for (uint32_t i = 0; i < size; ++i) {
        skip_element(valtype, transport);
      }
    } return;
  };

  char errbuf[128];
  sprintf(errbuf, "Unknown thrift typeID %ld", thrift_typeID);
  throw_tprotocolexception(errbuf, INVALID_DATA);
}

void protocol_writeMessageBegin(zval* transport, const char* method_name, int32_t msgtype, int32_t seqID) {
  zval args[3], ret, writeMessagefn;

  ZVAL_STRINGL(&args[0], (char*)method_name, strlen(method_name));
  ZVAL_LONG(&args[1], msgtype);
  ZVAL_LONG(&args[2], seqID);
  ZVAL_NULL(&ret);
  ZVAL_STRING(&writeMessagefn, "writeMessageBegin");

  call_user_function(EG(function_table), transport, &writeMessagefn, &ret, 3, args);

  zval_ptr_dtor(&args[0]);
  zval_ptr_dtor(&args[1]);
  zval_ptr_dtor(&args[2]);
  zval_dtor(&ret);
}

void binary_serialize_hashtable_key(int8_t keytype, PHPOutputTransport& transport, HashTable* ht, HashPosition& ht_pos) {
  bool keytype_is_numeric = (!((keytype == T_STRING) || (keytype == T_UTF8) || (keytype == T_UTF16)));

  zend_string *key;
  ulong index = 0;
  zval z, *z_p;
  int res;

  z_p = &z;
  ZVAL_UNDEF(&z);
  res = zend_hash_get_current_key_ex(ht, &key, &index, &ht_pos);
  if (keytype_is_numeric) {
    if (res == HASH_KEY_IS_STRING) {
      index = strtol(key->val, NULL, 10);
    }
    ZVAL_LONG(&z, index);
  } else {

    char buf[64];
    if (res == HASH_KEY_IS_STRING) {
      ZVAL_STRINGL(&z, key->val, key->len);
    } else {
      sprintf(buf, "%ld", index);
      ZVAL_STRINGL(&z, buf, strlen(buf));
    }

  }
  binary_serialize(keytype, transport, &z_p, NULL);
  zval_ptr_dtor(&z);
}

inline bool ttype_is_int(int8_t t) {
  return ((t == T_BYTE) || ((t >= T_I16)  && (t <= T_I64)));
}

inline bool ttypes_are_compatible(int8_t t1, int8_t t2) {
  // Integer types of different widths are considered compatible;
  // otherwise the typeID must match.
  return ((t1 == t2) || (ttype_is_int(t1) && ttype_is_int(t2)));
}

void binary_deserialize_spec(zval* zthis, PHPInputTransport& transport, HashTable* spec) {
  // SET and LIST have 'elem' => array('type', [optional] 'class')
  // MAP has 'val' => array('type', [optiona] 'class')
  zend_class_entry* ce = Z_OBJCE_P(zthis);
  while (true) {
    zval *val_ptr;

    int8_t ttype = transport.readI8();
    if (ttype == T_STOP) return;
    int16_t fieldno = transport.readI16();
    if ((val_ptr = zend_hash_index_find(spec, fieldno)) != NULL) {
      HashTable* fieldspec = Z_ARRVAL_P(val_ptr);
      // pull the field name
      // zend hash tables use the null at the end in the length... so strlen(hash key) + 1.
      val_ptr = zend_hash_str_find(fieldspec, ZEND_STRL("var"));
      char* varname = Z_STRVAL_P(val_ptr);

      // and the type
      val_ptr = zend_hash_str_find(fieldspec, ZEND_STRL("type"));
      if (Z_TYPE_P(val_ptr) != IS_LONG) convert_to_long(val_ptr);
      int8_t expected_ttype = Z_LVAL_P(val_ptr);

      if (ttypes_are_compatible(ttype, expected_ttype)) {
        zval rv;
        binary_deserialize(ttype, transport, &rv, fieldspec);
        zend_update_property(ce, zthis, varname, strlen(varname), &rv);
        zval_ptr_dtor(&rv);
      } else {
        skip_element(ttype, transport);
      }
    } else {
      skip_element(ttype, transport);
    }
  }
}

void binary_serialize(int8_t thrift_typeID, PHPOutputTransport& transport, zval** value, HashTable* fieldspec) {
  // At this point the typeID (and field num, if applicable) should've already been written to the output so all we need to do is write the payload.
  switch (thrift_typeID) {
    case T_STOP:
    case T_VOID:
      return;
    case T_STRUCT: {
      TSRMLS_FETCH();
      if (Z_TYPE_P(*value) != IS_OBJECT) {
        throw_tprotocolexception((char *)"Attempt to send non-object type as a T_STRUCT", INVALID_DATA);
      }
      zval* spec = zend_read_static_property(Z_OBJCE_P(*value), "_TSPEC", 6, false TSRMLS_CC);
      if (Z_TYPE_P(spec) != IS_ARRAY) {
        throw_tprotocolexception((char *) "Attempt to send non-Thrift object as a T_STRUCT", INVALID_DATA);
      }
      binary_serialize_spec(*value, transport, Z_ARRVAL_P(spec));
    } return;
    case T_BOOL:
      if (Z_TYPE_P(*value) != IS_TRUE && Z_TYPE_P(*value) != IS_FALSE) convert_to_boolean(*value);
      transport.writeI8(Z_TYPE_P(*value) == IS_TRUE ? 1 : 0);
      return;
    case T_BYTE:
      if (Z_TYPE_P(*value) != IS_LONG) convert_to_long(*value);
      transport.writeI8(Z_LVAL_P(*value));
      return;
    case T_I16:
      if (Z_TYPE_P(*value) != IS_LONG) convert_to_long(*value);
      transport.writeI16(Z_LVAL_P(*value));
      return;
    case T_I32:
      if (Z_TYPE_P(*value) != IS_LONG) convert_to_long(*value);
      transport.writeI32(Z_LVAL_P(*value));
      return;
    case T_I64:
    case T_U64: {
      int64_t l_data;
#if defined(_LP64) || defined(_WIN64)
      if (Z_TYPE_P(*value) != IS_LONG) convert_to_long(*value);
      l_data = Z_LVAL_P(*value);
#else
      if (Z_TYPE_P(*value) != IS_DOUBLE) convert_to_double(*value);
      l_data = (int64_t)Z_DVAL_P(*value);
#endif
      transport.writeI64(l_data);
    } return;
    case T_DOUBLE: {
      union {
        int64_t c;
        double d;
      } a;
      if (Z_TYPE_P(*value) != IS_DOUBLE) convert_to_double(*value);
      a.d = Z_DVAL_P(*value);
      transport.writeI64(a.c);
    } return;
    //case T_UTF7:
    case T_UTF8:
    case T_UTF16:
    case T_STRING:
      if (Z_TYPE_P(*value) != IS_STRING) convert_to_string(*value);
      transport.writeString(Z_STRVAL_P(*value), Z_STRLEN_P(*value));
      return;
    case T_MAP: {
      if (Z_TYPE_P(*value) != IS_ARRAY) convert_to_array(*value);
      if (Z_TYPE_P(*value) != IS_ARRAY) {
        throw_tprotocolexception((char *) "Attempt to send an incompatible type as an array (T_MAP)", INVALID_DATA);
      }
      HashTable* ht = Z_ARRVAL_P(*value);
      zval* val_ptr;

      val_ptr = zend_hash_str_find(fieldspec, ZEND_STRL("ktype"));
      if (Z_TYPE_P(val_ptr) != IS_LONG) convert_to_long(val_ptr);
      uint8_t keytype = Z_LVAL_P(val_ptr);
      transport.writeI8(keytype);
      zend_hash_str_find(fieldspec, ZEND_STRL("vtype"));
      if (Z_TYPE_P(val_ptr) != IS_LONG) convert_to_long(val_ptr);
      uint8_t valtype = Z_LVAL_P(val_ptr);
      transport.writeI8(valtype);

      val_ptr = zend_hash_str_find(fieldspec, ZEND_STRL("val"));
      HashTable* valspec = Z_ARRVAL_P(val_ptr);

      transport.writeI32(zend_hash_num_elements(ht));
      HashPosition key_ptr;
      for (zend_hash_internal_pointer_reset_ex(ht, &key_ptr);
           (val_ptr = zend_hash_get_current_data_ex(ht, &key_ptr)) != NULL;
           zend_hash_move_forward_ex(ht, &key_ptr)) {
        binary_serialize_hashtable_key(keytype, transport, ht, key_ptr);
        binary_serialize(valtype, transport, &val_ptr, valspec);
      }
    } return;
    case T_LIST: {
      if (Z_TYPE_P(*value) != IS_ARRAY) convert_to_array(*value);
      if (Z_TYPE_P(*value) != IS_ARRAY) {
        throw_tprotocolexception((char *) "Attempt to send an incompatible type as an array (T_LIST)", INVALID_DATA);
      }
      HashTable* ht = Z_ARRVAL_P(*value);
      zval* val_ptr;

      val_ptr = zend_hash_str_find(fieldspec, ZEND_STRL("etype"));
      if (Z_TYPE_P(val_ptr) != IS_LONG) convert_to_long(val_ptr);
      uint8_t valtype = Z_LVAL_P(val_ptr);
      transport.writeI8(valtype);

      val_ptr = zend_hash_str_find(fieldspec, ZEND_STRL("elem"));
      HashTable* valspec = Z_ARRVAL_P(val_ptr);

      transport.writeI32(zend_hash_num_elements(ht));
      HashPosition key_ptr;
      for (zend_hash_internal_pointer_reset_ex(ht, &key_ptr);
          (val_ptr = zend_hash_get_current_data_ex(ht, &key_ptr)) != NULL;
          zend_hash_move_forward_ex(ht, &key_ptr)) {
        binary_serialize(valtype, transport, &val_ptr, valspec);
      }
    } return;
    case T_SET: {
      if (Z_TYPE_P(*value) != IS_ARRAY) convert_to_array(*value);
      if (Z_TYPE_P(*value) != IS_ARRAY) {
        throw_tprotocolexception((char *) "Attempt to send an incompatible type as an array (T_SET)", INVALID_DATA);
      }
      HashTable* ht = Z_ARRVAL_P(*value);
      zval* val_ptr;

      val_ptr = zend_hash_str_find(fieldspec, ZEND_STRL("etype"));
      if (Z_TYPE_P(val_ptr) != IS_LONG) convert_to_long(val_ptr);
      uint8_t keytype = Z_LVAL_P(val_ptr);
      transport.writeI8(keytype);

      transport.writeI32(zend_hash_num_elements(ht));
      HashPosition key_ptr;
      for (zend_hash_internal_pointer_reset_ex(ht, &key_ptr);
           (val_ptr = zend_hash_get_current_data_ex(ht, &key_ptr)) != NULL;
           zend_hash_move_forward_ex(ht, &key_ptr)) {
        binary_serialize_hashtable_key(keytype, transport, ht, key_ptr);
      }
    } return;
  };
  char errbuf[128];
  sprintf(errbuf, "Unknown thrift typeID %d", thrift_typeID);
  throw_tprotocolexception(errbuf, INVALID_DATA);
}


void binary_serialize_spec(zval* zthis, PHPOutputTransport& transport, HashTable* spec) {
  HashPosition key_ptr;
  zval *val_ptr;

  TSRMLS_FETCH();
  zend_class_entry* ce = Z_OBJCE_P(zthis TSRMLS_CC);

  for (zend_hash_internal_pointer_reset_ex(spec, &key_ptr);
      (val_ptr = zend_hash_get_current_data_ex(spec, &key_ptr)) != NULL;
      zend_hash_move_forward_ex(spec, &key_ptr)) {
    ulong fieldno;
    zend_string *fieldstr;
    if (zend_hash_get_current_key_ex(spec, &fieldstr, &fieldno, &key_ptr) != HASH_KEY_IS_LONG) {
      throw_tprotocolexception((char *) "Bad keytype in TSPEC (expected 'long')", INVALID_DATA);
      return;
    }
    HashTable* fieldspec = Z_ARRVAL_P(val_ptr);

    // field name
    val_ptr = zend_hash_str_find(fieldspec, ZEND_STRL("var"));
    char* varname = Z_STRVAL_P(val_ptr);

    // thrift type
    val_ptr = zend_hash_str_find(fieldspec, ZEND_STRL("type"));
    if (Z_TYPE_P(val_ptr) != IS_LONG) convert_to_long(val_ptr);
    int8_t ttype = Z_LVAL_P(val_ptr);

    zval rv;
    zval* prop = zend_read_property(ce, zthis, varname, strlen(varname), false, &rv);
    if (Z_TYPE_P(prop) != IS_NULL) {
      transport.writeI8(ttype);
      transport.writeI16(fieldno);
      binary_serialize(ttype, transport, &prop, fieldspec);
    }
  }
  transport.writeI8(T_STOP); // struct end
}

// 6 params: $transport $method_name $ttype $request_struct $seqID $strict_write
PHP_FUNCTION(thrift_protocol_write_binary) {
  int argc = ZEND_NUM_ARGS();
  zval protocol, request_struct;
  if (argc < 6) {
    WRONG_PARAM_COUNT;
  }

  zval *args = (zval *)safe_emalloc(sizeof(zval), argc, 0);
  zend_get_parameters_array_ex(argc, args);

  if (Z_TYPE(args[0]) != IS_OBJECT) {
    php_error_docref(NULL TSRMLS_CC, E_ERROR, "1st parameter is not an object (transport)");
    efree(args);
    RETURN_NULL();
  }

  if (Z_TYPE(args[1]) != IS_STRING) {
    php_error_docref(NULL TSRMLS_CC, E_ERROR, "2nd parameter is not a string (method name)");
    efree(args);
    RETURN_NULL();
  }

  if (Z_TYPE(args[3]) != IS_OBJECT) {
    php_error_docref(NULL TSRMLS_CC, E_ERROR, "4th parameter is not an object (request struct)");
    efree(args);
    RETURN_NULL();
  }


  try {
    PHPOutputTransport transport(&args[0]);
    ZVAL_DUP(&protocol,  &args[0]);

    const char* method_name = Z_STRVAL(args[1]);

    convert_to_long(&args[2]);

    int32_t msgtype = Z_LVAL(args[2]);

    ZVAL_DUP(&request_struct, &args[3]);

    convert_to_long(&args[4]);
    int32_t seqID = Z_LVAL(args[4]);
    efree(args);
    args = NULL;
    protocol_writeMessageBegin(&protocol, method_name, msgtype, seqID);
    zval* spec = zend_read_static_property(Z_OBJCE_P(&request_struct TSRMLS_CC), "_TSPEC", 6, false TSRMLS_CC);
    if (Z_TYPE_P(spec) != IS_ARRAY) {
        throw_tprotocolexception((char *) "Attempt to send non-Thrift object", INVALID_DATA);
    }
    binary_serialize_spec(&request_struct, transport, Z_ARRVAL_P(spec));
    transport.flush();
  } catch (const PHPExceptionWrapper& ex) {
    zend_throw_exception_object(ex TSRMLS_CC);
    RETURN_NULL();
  } catch (const std::exception& ex) {
    throw_zend_exception_from_std_exception(ex TSRMLS_CC);
    RETURN_NULL();
  }
}

// 3 params: $transport $response_Typename $strict_read
PHP_FUNCTION(thrift_protocol_read_binary) {
  int argc = ZEND_NUM_ARGS();

  if (argc < 3) {
    WRONG_PARAM_COUNT;
  }

  zval *args = (zval *)safe_emalloc(sizeof(zval), argc, 0);
  zend_get_parameters_array_ex(argc, args);

  if (Z_TYPE(args[0]) != IS_OBJECT) {
    php_error_docref(NULL TSRMLS_CC, E_ERROR, "1st parameter is not an object (transport)");
    efree(args);
    RETURN_NULL();
  }

  if (Z_TYPE(args[1]) != IS_STRING) {
    php_error_docref(NULL TSRMLS_CC, E_ERROR, "2nd parameter is not a string (typename of expected response struct)");
    efree(args);
    RETURN_NULL();
  }

  try {
    PHPInputTransport transport(&args[0]);
    char* obj_typename = Z_STRVAL(args[1]);
    convert_to_boolean(&args[2]);
    bool strict_read = Z_TYPE(args[2]) == IS_TRUE;
    efree(args);
    args = NULL;

    int8_t messageType = 0;
    int32_t sz = transport.readI32();

    if (sz < 0) {
      // Check for correct version number
      int32_t version = sz & VERSION_MASK;
      if (version != VERSION_1) {
        throw_tprotocolexception((char *) "Bad version identifier", BAD_VERSION);
      }
      messageType = (sz & 0x000000ff);
      int32_t namelen = transport.readI32();
      // skip the name string and the sequence ID, we don't care about those
      transport.skip(namelen + 4);
    } else {
      if (strict_read) {
        throw_tprotocolexception((char *) "No version identifier... old protocol client in strict mode?", BAD_VERSION);
      } else {
        // Handle pre-versioned input
        transport.skip(sz); // skip string body
        messageType = transport.readI8();
        transport.skip(4); // skip sequence number
      }
    }

    if (messageType == T_EXCEPTION) {
      zval ex;
      createObject((char *) "\\Thrift\\Exception\\TApplicationException", &ex);
      zval* spec = zend_read_static_property(Z_OBJCE_P(&ex), "_TSPEC", 6, false TSRMLS_CC);
      binary_deserialize_spec(&ex, transport, Z_ARRVAL_P(spec));
      throw PHPExceptionWrapper(&ex);
    }

    createObject(obj_typename, return_value);
    zval* spec = zend_read_static_property(Z_OBJCE_P(return_value TSRMLS_CC), "_TSPEC", 6, false TSRMLS_CC);
    binary_deserialize_spec(return_value, transport, Z_ARRVAL_P(spec));
  } catch (const PHPExceptionWrapper& ex) {
    zend_throw_exception_object(ex TSRMLS_CC);
    RETURN_NULL();
  } catch (const std::exception& ex) {
    throw_zend_exception_from_std_exception(ex TSRMLS_CC);
    RETURN_NULL();
  }
}

