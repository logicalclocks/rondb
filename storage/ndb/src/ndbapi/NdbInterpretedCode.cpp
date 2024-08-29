/*
   Copyright (c) 2007, 2024, Oracle and/or its affiliates.
   Copyright (c) 2024, 2024, Hopsworks and/or its affiliates.

   This program is free software; you can redistribute it and/or modify
   it under the terms of the GNU General Public License, version 2.0,
   as published by the Free Software Foundation.

   This program is designed to work with certain software (including
   but not limited to OpenSSL) that is licensed under separate terms,
   as designated in a particular file or component or in included license
   documentation.  The authors of MySQL hereby grant you an additional
   permission to link the program and your derivative works with the
   separately licensed software that they have either included with
   the program or referenced in the documentation.

   This program is distributed in the hope that it will be useful,
   but WITHOUT ANY WARRANTY; without even the implied warranty of
   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
   GNU General Public License, version 2.0, for more details.

   You should have received a copy of the GNU General Public License
   along with this program; if not, write to the Free Software
   Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA 02110-1301  USA
*/

#include "NdbInterpretedCode.hpp"
#include <ndb_global.h>
#include "Interpreter.hpp"
#include "NdbDictionaryImpl.hpp"
#include "NdbRecord.hpp"

/*
   ToDo: We should add placeholders to this, so that one can use a single
   InterpretedCode object to create many operations from different embedded
   constants. Once we do this, we could add more error checks, as the cost
   would then not be paid for each operation creation.
   Suggest that fixed-size constants are replaced inline with latest values
   using instruction checking prior to send and new 'virtual' instructions
   which are resolved to real instructions at send time.  This requires a new
   interface to send parts of code, parts of placeholders and parts of other
   words.
   Var-sized constants can be used via a new branch_attr_op_arg instruction and
   use of some space in the subroutine section to store the constants.
   Wait for good use-case for placeholders.  Best currently is conflict
   detection, but the conflict detection programs are very short, so perhaps
   it's not worthwhile.
*/

NdbInterpretedCode::NdbInterpretedCode(const NdbDictionary::Table *table,
                                       Uint32 *buffer, Uint32 buffer_word_size)
    : m_table_impl(nullptr),
      m_buffer(buffer),
      m_buffer_length(buffer_word_size),
      m_internal_buffer(nullptr),
      m_number_of_labels(0),
      m_number_of_subs(0),
      m_number_of_calls(0),
      m_last_meta_pos(m_buffer_length),
      m_instructions_length(0),
      m_first_sub_instruction_pos(0),
      m_available_length(m_buffer_length),
      m_flags(0),
      m_error(),
      m_unknown_action(CmpHasNoUnknowns) {
  if (table != nullptr) m_table_impl = &NdbTableImpl::getImpl(*table);
  m_error.code = 0;
}

NdbInterpretedCode::NdbInterpretedCode(const NdbRecord &record, Uint32 *buffer,
                                       Uint32 buffer_word_size)
    : NdbInterpretedCode(nullptr, buffer, buffer_word_size) {
  m_table_impl = record.table;
}

NdbInterpretedCode::~NdbInterpretedCode() {
  if (m_internal_buffer != nullptr) {
    delete[] m_internal_buffer;
  }
}

void NdbInterpretedCode::set_sql_null_semantics(
    UnknownHandling unknown_action) {
  m_unknown_action = unknown_action;
}

void NdbInterpretedCode::reset() {
  if (m_internal_buffer != nullptr) {
    if (m_buffer == m_internal_buffer) {
      m_buffer = nullptr;
      m_buffer_length = 0;
    }
    delete[] m_internal_buffer;
    m_internal_buffer = nullptr;
  }
  m_number_of_labels = 0;
  m_number_of_subs = 0;
  m_number_of_calls = 0;
  m_last_meta_pos = m_buffer_length;
  m_instructions_length = 0;
  m_first_sub_instruction_pos = 0;
  m_available_length = m_buffer_length;
  m_flags = 0;
  m_error.code = 0;
}

int NdbInterpretedCode::error(Uint32 code) {
  m_flags |= GotError;
  m_error.code = code;
  return -1;
}

/* Make sure there's space for the number of words
 * specified between the end of the code and the
 * start of the meta information or return false.
 * This method dynamically doubles the internal buffer
 * length if the caller did not supply a buffer.
 */
bool NdbInterpretedCode::have_space_for(Uint32 wordsRequired) {
  assert(m_last_meta_pos <= m_buffer_length);
  assert(m_last_meta_pos >= m_instructions_length);
  assert(m_available_length == m_last_meta_pos - m_instructions_length);
  if (likely(m_available_length >= wordsRequired)) return true;

  if ((m_internal_buffer != nullptr) || (m_buffer_length == 0)) {
    Uint32 newSize = m_buffer_length;
    const Uint32 extraRequired = wordsRequired - m_available_length;

    /* Initial allocation of dynamic buffer */
    if (newSize == 0) newSize = 1;

    do {
      /* Double buffer length until there's enough space, or
       * we reach the maximum size
       */
      newSize = newSize << 1;
    } while (((newSize - m_buffer_length) < extraRequired) &&
             (newSize < MaxDynamicBufSize));

    if (newSize > MaxDynamicBufSize) newSize = MaxDynamicBufSize;

    /* Were we able to get enough extra space? */
    if ((newSize - m_buffer_length) >= extraRequired) {
      Uint32 *newBuf = new Uint32[newSize];

      if (newBuf != nullptr) {
        Uint32 metaInfoWords = m_buffer_length - m_last_meta_pos;
        Uint32 newLastMetaInfoPos = newSize - metaInfoWords;

        if (m_buffer_length > 0) {
          /* Copy instruction words to start of new buffer */
          memcpy(newBuf, m_internal_buffer, m_instructions_length << 2);

          /* Copy metainfo words to end of new buffer */
          memcpy(&newBuf[newLastMetaInfoPos], &m_buffer[m_last_meta_pos],
                 metaInfoWords << 2);

          delete[] m_internal_buffer;
        }

        m_buffer = m_internal_buffer = newBuf;
        m_available_length += (newSize - m_buffer_length);
        m_buffer_length = newSize;
        m_last_meta_pos = newLastMetaInfoPos;

        return true;
      }
    }
  }
  return false;
}

inline int NdbInterpretedCode::add1(Uint32 x1) {
  if (unlikely(!have_space_for(1))) return error(TooManyInstructions);

  Uint32 current = m_instructions_length;
  m_buffer[current] = x1;
  m_instructions_length = current + 1;
  m_available_length--;
  return 0;
}

inline int NdbInterpretedCode::add2(Uint32 x1, Uint32 x2) {
  if (unlikely(!have_space_for(2))) return error(TooManyInstructions);
  Uint32 current = m_instructions_length;
  m_buffer[current] = x1;
  m_buffer[current + 1] = x2;
  m_instructions_length = current + 2;
  m_available_length -= 2;
  return 0;
}

inline int NdbInterpretedCode::add3(Uint32 x1, Uint32 x2, Uint32 x3) {
  if (unlikely(!have_space_for(3))) return error(TooManyInstructions);
  Uint32 current = m_instructions_length;
  m_buffer[current] = x1;
  m_buffer[current + 1] = x2;
  m_buffer[current + 2] = x3;
  m_instructions_length = current + 3;
  m_available_length -= 3;
  return 0;
}

inline int NdbInterpretedCode::addN(const Uint32 *data, Uint32 length) {
  if (likely(length > 0)) {
    if (unlikely(!have_space_for(length))) return error(TooManyInstructions);

    /* data* may be unaligned, so we do a byte copy
     * using memcpy
     */
    memcpy(&m_buffer[m_instructions_length], data, length << 2);

    m_instructions_length += length;
    m_available_length -= length;
  }
  return 0;
}

inline int NdbInterpretedCode::addMeta(CodeMetaInfo &info) {
  if (unlikely(!have_space_for(CODEMETAINFO_WORDS)))
    return error(TooManyInstructions);

  m_buffer[--m_last_meta_pos] = (Uint32)info.number << 16 | info.type;
  m_buffer[--m_last_meta_pos] = info.firstInstrPos;

  m_available_length -= CODEMETAINFO_WORDS;

  return 0;
}

int
NdbInterpretedCode::add_reg(Uint32 RegDest, 
                            Uint32 RegSource1, Uint32 RegSource2) {
  if ((RegDest >= MaxReg) ||
      (RegSource1 >= MaxReg) ||
      (RegSource2 >= MaxReg))
    return error(BadRegister);
  return add1(Interpreter::Add(RegDest, RegSource1,
                               RegSource2));
}

int
NdbInterpretedCode::sub_reg(Uint32 RegDest,
                            Uint32 RegSource1, Uint32 RegSource2) {
  if ((RegDest >= MaxReg) ||
      (RegSource1 >= MaxReg) ||
      (RegSource2 >= MaxReg))
    return error(BadRegister);
  return add1(Interpreter::Sub(RegDest, RegSource1,
                               RegSource2));
}

int
NdbInterpretedCode::lshift_reg(Uint32 RegDest,
                               Uint32 RegSource1, Uint32 RegSource2) {
  if ((RegDest >= MaxReg) ||
      (RegSource1 >= MaxReg) ||
      (RegSource2 >= MaxReg))
    return error(BadRegister);
  return add1(Interpreter::Lshift(RegDest, RegSource1,
                                  RegSource2));
}

int
NdbInterpretedCode::rshift_reg(Uint32 RegDest,
                               Uint32 RegSource1, Uint32 RegSource2) {
  if ((RegDest >= MaxReg) ||
      (RegSource1 >= MaxReg) ||
      (RegSource2 >= MaxReg))
    return error(BadRegister);
  return add1(Interpreter::Rshift(RegDest, RegSource1,
                                  RegSource2));
}

int
NdbInterpretedCode::mul_reg(Uint32 RegDest,
                            Uint32 RegSource1, Uint32 RegSource2) {
  if ((RegDest >= MaxReg) ||
      (RegSource1 >= MaxReg) ||
      (RegSource2 >= MaxReg))
    return error(BadRegister);
  return add1(Interpreter::Mul(RegDest, RegSource1,
                               RegSource2));
}

int
NdbInterpretedCode::div_reg(Uint32 RegDest,
                            Uint32 RegSource1, Uint32 RegSource2) {
  if ((RegDest >= MaxReg) ||
      (RegSource1 >= MaxReg) ||
      (RegSource2 >= MaxReg))
    return error(BadRegister);
  return add1(Interpreter::Div(RegDest, RegSource1,
                               RegSource2));
}

int
NdbInterpretedCode::and_reg(Uint32 RegDest,
                            Uint32 RegSource1, Uint32 RegSource2) {
  if ((RegDest >= MaxReg) ||
      (RegSource1 >= MaxReg) ||
      (RegSource2 >= MaxReg))
    return error(BadRegister);
  return add1(Interpreter::And(RegDest, RegSource1,
                               RegSource2));
}

int
NdbInterpretedCode::or_reg(Uint32 RegDest,
                           Uint32 RegSource1, Uint32 RegSource2) {
  if ((RegDest >= MaxReg) ||
      (RegSource1 >= MaxReg) ||
      (RegSource2 >= MaxReg))
    return error(BadRegister);
  return add1(Interpreter::Or(RegDest, RegSource1,
                              RegSource2));
}

int
NdbInterpretedCode::xor_reg(Uint32 RegDest,
                            Uint32 RegSource1, Uint32 RegSource2) {
  if ((RegDest >= MaxReg) ||
      (RegSource1 >= MaxReg) ||
      (RegSource2 >= MaxReg))
    return error(BadRegister);
  return add1(Interpreter::Xor(RegDest, RegSource1,
                               RegSource2));
}

int
NdbInterpretedCode::mod_reg(Uint32 RegDest,
                            Uint32 RegSource1, Uint32 RegSource2) {
  if ((RegDest >= MaxReg) ||
      (RegSource1 >= MaxReg) ||
      (RegSource2 >= MaxReg))
    return error(BadRegister);
  return add1(Interpreter::Mod(RegDest, RegSource1,
                               RegSource2));
}

int
NdbInterpretedCode::not_reg(Uint32 RegDest,
                            Uint32 RegSource1) {
  if ((RegDest >= MaxReg) ||
      (RegSource1 >= MaxReg))
    return error(BadRegister);
  return add1(Interpreter::Not(RegDest, RegSource1));
}

int
NdbInterpretedCode::move_reg(Uint32 RegDest,
                             Uint32 RegSource1) {
  if ((RegDest >= MaxReg) ||
      (RegSource1 >= MaxReg))
    return error(BadRegister);
  return add1(Interpreter::AddC(RegDest,
                                RegSource1,
                                Uint16(0)));
}

int
NdbInterpretedCode::add_const_reg(Uint32 RegDest, 
                                  Uint32 RegSource1,
                                  Uint16 Constant) {
  if ((RegDest >= MaxReg) ||
      (RegSource1 >= MaxReg))
    return error(BadRegister);
  return add1(Interpreter::AddC(RegDest, RegSource1,
                                Constant));
}

int
NdbInterpretedCode::sub_const_reg(Uint32 RegDest,
                                  Uint32 RegSource1,
                                  Uint16 Constant) {
  if ((RegDest >= MaxReg) ||
      (RegSource1 >= MaxReg))
    return error(BadRegister);
  return add1(Interpreter::SubC(RegDest, RegSource1,
                                Constant));
}

int
NdbInterpretedCode::lshift_const_reg(Uint32 RegDest,
                                     Uint32 RegSource1,
                                     Uint16 Constant) {
  if ((RegDest >= MaxReg) ||
      (RegSource1 >= MaxReg))
    return error(BadRegister);
  return add1(Interpreter::LshiftC(RegDest, RegSource1,
                                   Constant));
}

int
NdbInterpretedCode::rshift_const_reg(Uint32 RegDest,
                                     Uint32 RegSource1,
                                     Uint16 Constant) {
  if ((RegDest >= MaxReg) ||
      (RegSource1 >= MaxReg))
    return error(BadRegister);
  return add1(Interpreter::RshiftC(RegDest, RegSource1,
                                   Constant));
}

int
NdbInterpretedCode::mul_const_reg(Uint32 RegDest,
                                  Uint32 RegSource1,
                                  Uint16 Constant) {
  if ((RegDest >= MaxReg) ||
      (RegSource1 >= MaxReg))
    return error(BadRegister);
  return add1(Interpreter::MulC(RegDest, RegSource1,
                                Constant));
}

int
NdbInterpretedCode::div_const_reg(Uint32 RegDest,
                                  Uint32 RegSource1,
                                  Uint16 Constant) {
  if ((RegDest >= MaxReg) ||
      (RegSource1 >= MaxReg))
    return error(BadRegister);
  return add1(Interpreter::DivC(RegDest, RegSource1,
                                Constant));
}

int
NdbInterpretedCode::and_const_reg(Uint32 RegDest,
                                  Uint32 RegSource1,
                                  Uint16 Constant) {
  if ((RegDest >= MaxReg) ||
      (RegSource1 >= MaxReg))
    return error(BadRegister);
  return add1(Interpreter::AndC(RegDest, RegSource1,
                                Constant));
}

int
NdbInterpretedCode::or_const_reg(Uint32 RegDest,
                                 Uint32 RegSource1,
                                 Uint16 Constant) {
  if ((RegDest >= MaxReg) ||
      (RegSource1 >= MaxReg))
    return error(BadRegister);
  return add1(Interpreter::OrC(RegDest, RegSource1,
                               Constant));
}

int
NdbInterpretedCode::xor_const_reg(Uint32 RegDest,
                                  Uint32 RegSource1,
                                  Uint16 Constant) {
  if ((RegDest >= MaxReg) ||
      (RegSource1 >= MaxReg))
    return error(BadRegister);
  return add1(Interpreter::XorC(RegDest, RegSource1,
                                Constant));
}

int
NdbInterpretedCode::mod_const_reg(Uint32 RegDest,
                                  Uint32 RegSource1,
                                  Uint16 Constant) {
  if ((RegDest >= MaxReg) ||
      (RegSource1 >= MaxReg))
    return error(BadRegister);
  return add1(Interpreter::ModC(RegDest, RegSource1,
                                Constant));
}

int
NdbInterpretedCode::write_interpreter_output(Uint32 RegValue,
                                             Uint32 OutputIndex) {
  if (RegValue >= MaxReg)
    return error(BadRegister);
  if (OutputIndex >= MaxOutputIndex)
    return error(BadOutputIndex);
  return add1(Interpreter::WriteInterpreterOutput(
              RegValue,
              OutputIndex));
}

int
NdbInterpretedCode::convert_size(Uint32 RegSizeDest, Uint32 RegOffset) {
  if ((RegSizeDest >= MaxReg) ||
      (RegOffset >= MaxReg))
    return error(BadRegister);
  return add1(Interpreter::ConvertSize(
              RegSizeDest,
              RegOffset));
}

int
NdbInterpretedCode::write_size_mem(Uint32 RegSizeDest, Uint32 RegOffset) {
  if ((RegSizeDest >= MaxReg) ||
      (RegOffset >= MaxReg))
    return error(BadRegister);
  return add1(Interpreter::WriteSizeMem(
              RegSizeDest,
              RegOffset));
}

int
NdbInterpretedCode::str_to_int64(Uint32 RegDestValue,
                                 Uint32 RegOffset,
                                 Uint32 RegSize) {
  if ((RegDestValue >= MaxReg) ||
      (RegOffset >= MaxReg) ||
      (RegSize >= MaxReg))
    return error(BadRegister);
  return add1(Interpreter::StrToInt64(
              RegDestValue,
              RegOffset,
              RegSize));
}

int
NdbInterpretedCode::int64_to_str(Uint32 RegDestSize,
                                 Uint32 RegOffset,
                                 Uint32 RegValue) {
  if ((RegDestSize >= MaxReg) ||
      (RegOffset >= MaxReg) ||
      (RegValue >= MaxReg))
    return error(BadRegister);
  return add1(Interpreter::Int64ToStr(
              RegDestSize,
              RegOffset,
              RegValue));
}

int
NdbInterpretedCode::read_uint8_to_reg_const(Uint32 RegDest,
                                            Uint32 Offset) {
  if (RegDest >= MaxReg)
    return error(BadRegister);
  return add1(Interpreter::ReadUint8FromMemIntoRegConst(
              RegDest,
              Offset));
}

int
NdbInterpretedCode::read_uint16_to_reg_const(Uint32 RegDest,
                                             Uint32 Offset) {
  if (RegDest >= MaxReg)
    return error(BadRegister);
  return add1(Interpreter::ReadUint16FromMemIntoRegConst(
              RegDest,
              Offset));
}

int
NdbInterpretedCode::read_uint32_to_reg_const(Uint32 RegDest,
                                             Uint32 Offset) {
  if (RegDest >= MaxReg)
    return error(BadRegister);
  return add1(Interpreter::ReadUint32FromMemIntoRegConst(
              RegDest,
              Offset));
}

int
NdbInterpretedCode::read_int64_to_reg_const(Uint32 RegDest,
                                            Uint32 Offset) {
  if (RegDest >= MaxReg)
    return error(BadRegister);
  return add1(Interpreter::ReadInt64FromMemIntoRegConst(
              RegDest,
              Offset));
}

int
NdbInterpretedCode::read_uint8_to_reg_reg(Uint32 RegDest,
                                          Uint32 RegOffset) {
  if (RegDest >= MaxReg)
    return error(BadRegister);
  return add1(Interpreter::ReadUint8FromMemIntoRegReg(
              RegDest,
              RegOffset));
}

int
NdbInterpretedCode::read_uint16_to_reg_reg(Uint32 RegDest,
                                           Uint32 RegOffset) {
  if ((RegDest >= MaxReg) ||
      (RegOffset >= MaxReg))
    return error(BadRegister);
  return add1(Interpreter::ReadUint16FromMemIntoRegReg(
              RegDest,
              RegOffset));
}

int
NdbInterpretedCode::read_uint32_to_reg_reg(Uint32 RegDest,
                                           Uint32 RegOffset) {
  if ((RegDest >= MaxReg) ||
      (RegOffset >= MaxReg))
    return error(BadRegister);
  return add1(Interpreter::ReadUint32FromMemIntoRegReg(
              RegDest,
              RegOffset));
}

int
NdbInterpretedCode::read_int64_to_reg_reg(Uint32 RegDest,
                                          Uint32 RegOffset) {
  if ((RegDest >= MaxReg) ||
      (RegOffset >= MaxReg))
    return error(BadRegister);
  return add1(Interpreter::ReadInt64FromMemIntoRegReg(
              RegDest,
              RegOffset));
}

int
NdbInterpretedCode::write_uint8_reg_to_mem_const(Uint32 RegSource,
                                                 Uint16 Constant) {
  if (RegSource >= MaxReg)
    return error(BadRegister);
  return add1(Interpreter::WriteUint8RegIntoMemConst(
              RegSource,
              Constant));
}

int
NdbInterpretedCode::write_uint16_reg_to_mem_const(Uint32 RegSource,
                                                  Uint16 Constant) {
  if (RegSource >= MaxReg)
    return error(BadRegister);
  return add1(Interpreter::WriteUint16RegIntoMemConst(
              RegSource,
              Constant));
}

int
NdbInterpretedCode::write_uint32_reg_to_mem_const(Uint32 RegSource,
                                                  Uint16 Constant) {
  if (RegSource >= MaxReg)
    return error(BadRegister);
  return add1(Interpreter::WriteUint32RegIntoMemConst(
              RegSource,
              Constant));
}

int
NdbInterpretedCode::write_int64_reg_to_mem_const(Uint32 RegSource,
                                                 Uint16 Constant) {
  if (RegSource >= MaxReg)
    return error(BadRegister);
  return add1(Interpreter::WriteInt64RegIntoMemConst(
              RegSource,
              Constant));
}


int
NdbInterpretedCode::write_uint8_reg_to_mem_reg(Uint32 RegSource,
                                               Uint32 RegOffset) {
  if ((RegSource >= MaxReg) ||
      (RegOffset >= MaxReg))
    return error(BadRegister);
  return add1(Interpreter::WriteUint8RegIntoMemReg(
              RegSource,
              RegOffset));
}

int
NdbInterpretedCode::write_uint16_reg_to_mem_reg(Uint32 RegSource,
                                                Uint32 RegOffset) {
  if ((RegSource >= MaxReg) ||
      (RegOffset >= MaxReg))
    return error(BadRegister);
  return add1(Interpreter::WriteUint16RegIntoMemReg(
              RegSource,
              RegOffset));
}

int
NdbInterpretedCode::write_uint32_reg_to_mem_reg(Uint32 RegSource,
                                                Uint32 RegOffset) {
  if ((RegSource >= MaxReg) ||
      (RegOffset >= MaxReg))
    return error(BadRegister);
  return add1(Interpreter::WriteUint32RegIntoMemReg(
              RegSource,
              RegOffset));
}

int
NdbInterpretedCode::write_int64_reg_to_mem_reg(Uint32 RegSource,
                                               Uint32 RegOffset) {
  if ((RegSource >= MaxReg) ||
      (RegOffset >= MaxReg))
    return error(BadRegister);
  return add1(Interpreter::WriteInt64RegIntoMemReg(
              RegSource,
              RegOffset));
}

int
NdbInterpretedCode::load_const_u32(Uint32 RegDest, Uint32 Constant) {
  if (RegDest >= MaxReg)
    return error(BadRegister);
  return add2(Interpreter::LoadConst32(RegDest), Constant);
}

int NdbInterpretedCode::load_const_u64(Uint32 RegDest, Uint64 Constant) {
  union {
    Uint64 val64;
    Uint32 val32[2];
  };
  val64 = Constant;
  if (RegDest >= MaxReg)
    return error(BadRegister);
  return add3(Interpreter::LoadConst64(RegDest), val32[0], val32[1]);
}

int
NdbInterpretedCode::load_const_null(Uint32 RegDest) {
  if (RegDest >= MaxReg)
    return error(BadRegister);
  return add1(Interpreter::LoadNull(RegDest));
}

int
NdbInterpretedCode::load_const_u16(Uint32 RegDest, Uint32 Constant) {
  if (RegDest >= MaxReg)
    return error(BadRegister);
  return add1(Interpreter::LoadConst16((RegDest), Constant));
}

static inline
void
zero32(Uint8* dstPtr, const Uint32 len) {
  Uint32 odd = len & 3;
  if (odd != 0)
  {
    Uint32 aligned = len & ~3;
    Uint8* dst = dstPtr+aligned;
    switch(odd){     /* odd is: {1..3} */
    case 1:
      dst[1] = 0;
      [[fallthrough]];
    case 2:
      dst[2] = 0;
      [[fallthrough]];
    default:         /* Known to be odd==3 */
      dst[3] = 0;
    }
  }
} 

int
NdbInterpretedCode::load_const_mem(Uint32 RegMemoryOffset,
                                   Uint32 RegDestSize,
                                   Uint16 ConstantSize,
                                   Uint32 *const_memory) {
  int ret_code;
  zero32((Uint8*)const_memory, ConstantSize);
  if ((RegMemoryOffset >= MaxReg) ||
      (RegDestSize >= MaxReg))
    return error(BadRegister);
  if ((ret_code = add1(Interpreter::LoadConstMem(
       RegMemoryOffset,
       RegDestSize,
       ConstantSize))))
  {
    return ret_code;
  }
  Uint32 words = (ConstantSize + 3) / 4;
  return addN(const_memory, words);
}

int
NdbInterpretedCode::read_attr_impl(const NdbColumnImpl *c, Uint32 RegDest) {
  if (RegDest >= MaxReg)
    return error(BadRegister);
  if (c->m_storageType == NDB_STORAGETYPE_DISK)
    m_flags|= UsesDisk;
  return add1(Interpreter::Read(c->m_attrId, RegDest));
}

int
NdbInterpretedCode::read_partial_impl(const NdbColumnImpl *c,
                                      Uint32 RegMemoryOffset,
                                      Uint32 RegPos,
                                      Uint32 RegSize,
                                      Uint32 RegDest) {
  if ((RegMemoryOffset >= MaxReg) ||
      (RegPos >= MaxReg) ||
      (RegSize >= MaxReg) ||
      (RegDest >= MaxReg))
    return error(BadRegister);
  if (c->m_storageType == NDB_STORAGETYPE_DISK)
    m_flags|= UsesDisk;
  return add1(Interpreter::ReadPartial(c->m_attrId,
                                       RegMemoryOffset,
                                       RegPos,
                                       RegSize,
                                       RegDest));
}

int
NdbInterpretedCode::read_partial(Uint32 attrId,
                                 Uint32 RegMemoryOffset,
                                 Uint32 RegPos,
                                 Uint32 RegSize,
                                 Uint32 RegDest) {
  if (unlikely(m_table_impl == nullptr))
    /* NdbInterpretedCode instruction requires that table is set */
    return error(4538);
  const NdbColumnImpl *c= m_table_impl->getColumn(attrId);
  if (unlikely(c == nullptr))
    return error(BadAttributeId);
  return read_partial_impl(c,
                           RegMemoryOffset,
                           RegPos,
                           RegSize,
                           RegDest);
}

int
NdbInterpretedCode::read_partial(const NdbDictionary::Column *column,
                                 Uint32 RegMemOffset,
                                 Uint32 RegPos,
                                 Uint32 RegSize,
                                 Uint32 RegDest) {
  if (unlikely(m_table_impl == nullptr))
    /* NdbInterpretedCode instruction requires that table is set */
    return error(4538);
  // TODO : Check column is from the correct table
  return read_partial_impl(&NdbColumnImpl::getImpl(*column),
                           RegMemOffset,
                           RegPos,
                           RegSize,
                           RegDest);
}

int
NdbInterpretedCode::read_full_impl(const NdbColumnImpl *c,
                                   Uint32 RegMemoryOffset,
                                   Uint32 RegDest) {
  if ((RegMemoryOffset >= MaxReg) ||
      (RegDest >= MaxReg))
    return error(BadRegister);
  if (c->m_storageType == NDB_STORAGETYPE_DISK)
    m_flags|= UsesDisk;
  return add1(Interpreter::ReadFull(c->m_attrId,
                                    RegMemoryOffset,
                                    RegDest));
}

int
NdbInterpretedCode::read_full(Uint32 attrId,
                              Uint32 RegMemoryOffset,
                              Uint32 RegDest) {
  if (unlikely(m_table_impl == nullptr))
    /* NdbInterpretedCode instruction requires that table is set */
    return error(4538);
  const NdbColumnImpl *c= m_table_impl->getColumn(attrId);
  if (unlikely(c == nullptr))
    return error(BadAttributeId);
  return read_full_impl(c,
                        RegMemoryOffset,
                        RegDest);
}

int
NdbInterpretedCode::read_full(const NdbDictionary::Column *column,
                              Uint32 RegMemOffset,
                              Uint32 RegDest) {
  if (unlikely(m_table_impl == nullptr))
    /* NdbInterpretedCode instruction requires that table is set */
    return error(4538);
  // TODO : Check column is from the correct table
  return read_full_impl(&NdbColumnImpl::getImpl(*column),
                        RegMemOffset,
                        RegDest);
}

int NdbInterpretedCode::read_attr(Uint32 RegDest, Uint32 attrId) {
  if (unlikely(m_table_impl == nullptr))
    /* NdbInterpretedCode instruction requires that table is set */
    return error(4538);
  const NdbColumnImpl *c = m_table_impl->getColumn(attrId);
  if (unlikely(c == nullptr)) return error(BadAttributeId);
  return read_attr_impl(c, RegDest);
}

int NdbInterpretedCode::read_attr(Uint32 RegDest,
                                  const NdbDictionary::Column *column) {
  if (unlikely(m_table_impl == nullptr))
    /* NdbInterpretedCode instruction requires that table is set */
    return error(4538);
  // TODO : Check column is from the correct table
  return read_attr_impl(&NdbColumnImpl::getImpl(*column),
                        RegDest);
}

int
NdbInterpretedCode::write_from_mem_impl(const NdbColumnImpl *c,
                                        Uint32 RegMemoryOffset,
                                        Uint32 RegSize) {
  if ((RegMemoryOffset >= MaxReg) ||
      (RegSize >= MaxReg))
    return error(BadRegister);
  if (c->m_storageType == NDB_STORAGETYPE_DISK)
    m_flags|= UsesDisk;
  return add1(Interpreter::WriteFromMem(c->m_attrId,
                                        RegMemoryOffset,
                                        RegSize));
}

int
NdbInterpretedCode::write_from_mem(Uint32 attrId,
                                   Uint32 RegMemOffset,
                                   Uint32 RegSize) {
  if (unlikely(m_table_impl == nullptr))
    /* NdbInterpretedCode instruction requires that table is set */
    return error(4538);
  const NdbColumnImpl *c= m_table_impl->getColumn(attrId);
  if (unlikely(c == nullptr))
    return error(BadAttributeId);
  return write_from_mem_impl(c, RegMemOffset, RegSize);
}

int
NdbInterpretedCode::write_from_mem(const NdbDictionary::Column *column,
                                   Uint32 RegMemOffset,
                                   Uint32 RegSize) {
  if (unlikely(m_table_impl == nullptr))
    /* NdbInterpretedCode instruction requires that table is set */
    return error(4538);
  // TODO : Check column is from the right table
  return write_from_mem_impl(&NdbColumnImpl::getImpl(*column),
                             RegMemOffset, RegSize);
}

int
NdbInterpretedCode::write_partial_from_mem_impl(const NdbColumnImpl *c,
                                                Uint32 RegMemoryOffset,
                                                Uint32 RegSize,
                                                Uint32 RegStartPos) {
  if ((RegMemoryOffset >= MaxReg) ||
      (RegSize >= MaxReg ||
      (RegStartPos >= MaxReg)))
    return error(BadRegister);
  if (c->m_storageType == NDB_STORAGETYPE_DISK)
    m_flags|= UsesDisk;
  return add1(Interpreter::WritePartialFromMem(c->m_attrId,
                                               RegMemoryOffset,
                                               RegSize,
                                               RegStartPos));
}

int
NdbInterpretedCode::write_partial_from_mem(Uint32 attrId,
                                           Uint32 RegMemOffset,
                                           Uint32 RegSize,
                                           Uint32 RegStartPos) {
  if (unlikely(m_table_impl == nullptr))
    /* NdbInterpretedCode instruction requires that table is set */
    return error(4538);
  const NdbColumnImpl *c= m_table_impl->getColumn(attrId);
  if (unlikely(c == nullptr))
    return error(BadAttributeId);
  return write_partial_from_mem_impl(c, RegMemOffset, RegSize, RegStartPos);
}

int
NdbInterpretedCode::write_partial_from_mem(const NdbDictionary::Column *column,
                                           Uint32 RegMemOffset,
                                           Uint32 RegSize,
                                           Uint32 RegStartPos) {
  if (unlikely(m_table_impl == nullptr))
    /* NdbInterpretedCode instruction requires that table is set */
    return error(4538);
  // TODO : Check column is from the right table
  return write_partial_from_mem_impl(&NdbColumnImpl::getImpl(*column),
                                     RegMemOffset, RegSize, RegStartPos);
}

int
NdbInterpretedCode::append_from_mem_impl(const NdbColumnImpl *c,
                                         Uint32 RegMemoryOffset,
                                         Uint32 RegSize) {
  if ((RegMemoryOffset >= MaxReg) ||
      (RegSize >= MaxReg))
    return error(BadRegister);
  if (c->m_storageType == NDB_STORAGETYPE_DISK)
    m_flags|= UsesDisk;
  return add1(Interpreter::AppendFromMem(c->m_attrId,
                                         RegMemoryOffset,
                                         RegSize));
}

int
NdbInterpretedCode::append_from_mem(Uint32 attrId,
                                    Uint32 RegMemOffset,
                                    Uint32 RegSize) {
  if (unlikely(m_table_impl == nullptr))
    /* NdbInterpretedCode instruction requires that table is set */
    return error(4538);
  const NdbColumnImpl *c= m_table_impl->getColumn(attrId);
  if (unlikely(c == nullptr))
    return error(BadAttributeId);
  return append_from_mem_impl(c, RegMemOffset, RegSize);
}

int
NdbInterpretedCode::append_from_mem(const NdbDictionary::Column *column,
                                    Uint32 RegMemOffset,
                                    Uint32 RegSize) {
  if (unlikely(m_table_impl == nullptr))
    /* NdbInterpretedCode instruction requires that table is set */
    return error(4538);
  // TODO : Check column is from the right table
  return append_from_mem_impl(&NdbColumnImpl::getImpl(*column),
                              RegMemOffset, RegSize);
}

int
NdbInterpretedCode::write_attr_impl(const NdbColumnImpl *c, Uint32 RegSource) {
  if (RegSource >= MaxReg)
    return error(BadRegister);
  if (c->m_storageType == NDB_STORAGETYPE_DISK)
    m_flags |= UsesDisk;
  return add1(Interpreter::Write(c->m_attrId, RegSource));
}


int
NdbInterpretedCode::write_attr(Uint32 attrId, Uint32 RegSource) {
  if (unlikely(m_table_impl == nullptr))
    /* NdbInterpretedCode instruction requires that table is set */
    return error(4538);
  const NdbColumnImpl *c = m_table_impl->getColumn(attrId);
  if (unlikely(c == nullptr)) return error(BadAttributeId);
  return write_attr_impl(c, RegSource);
}

int NdbInterpretedCode::write_attr(const NdbDictionary::Column *column,
                                   Uint32 RegSource) {
  if (unlikely(m_table_impl == nullptr))
    /* NdbInterpretedCode instruction requires that table is set */
    return error(4538);
  // TODO : Check column is from the right table
  return write_attr_impl(&NdbColumnImpl::getImpl(*column),
                         RegSource);
}

int NdbInterpretedCode::def_label(int LabelNum) {
  if (LabelNum < 0 || (Uint32)LabelNum > MaxLabels) return error(BadLabelNum);

  m_number_of_labels++;

  CodeMetaInfo info;

  info.type = Label;
  info.number = LabelNum;
  info.firstInstrPos = m_instructions_length;

  // Note, no check for whether the label's already defined here.
  return addMeta(info);
}

int NdbInterpretedCode::add_branch(Uint32 instruction, Uint32 label) {
  /* We store the instruction with the label as the offset
   * rather than the correct offset.
   * This is corrected at finalise() time when we know
   * the correct offset for the code
   */
  if (unlikely(label > 0xffff)) return error(BranchToBadLabel);
  return add1(instruction | label << 16);
}

int NdbInterpretedCode::branch_label(Uint32 label) {
  return add_branch(Interpreter::BRANCH, label);
}

/* For the following inequalities, the order of the
 * registers passed to Interpreter::Branch is reversed
 * to correct the reordering done in Interpreter::Branch
 * This ensures that the comparison is Lvalue <cond> Rvalue,
 * not Rvalue <cond> Lvalue.
 */
int NdbInterpretedCode::branch_ge(Uint32 RegLvalue,
                                  Uint32 RegRvalue,
                                  Uint32 label) {
  if ((RegLvalue >= MaxReg) ||
      (RegRvalue >= MaxReg))
    return error(BadRegister);
  Uint32 instr = Interpreter::Branch(Interpreter::BRANCH_GE_REG_REG,
                                     RegRvalue,
                                     RegLvalue);
  return add_branch(instr, label);
}

int NdbInterpretedCode::branch_gt(Uint32 RegLvalue,
                                  Uint32 RegRvalue,
                                  Uint32 label) {
  if ((RegLvalue >= MaxReg) ||
      (RegRvalue >= MaxReg))
    return error(BadRegister);
  Uint32 instr = Interpreter::Branch(Interpreter::BRANCH_GT_REG_REG,
                                     RegRvalue,
                                     RegLvalue);
  return add_branch(instr, label);
}

int NdbInterpretedCode::branch_le(Uint32 RegLvalue,
                                  Uint32 RegRvalue,
                                  Uint32 label) {
  if ((RegLvalue >= MaxReg) ||
      (RegRvalue >= MaxReg))
    return error(BadRegister);
  Uint32 instr = Interpreter::Branch(Interpreter::BRANCH_LE_REG_REG,
                                     RegRvalue,
                                     RegLvalue);
  return add_branch(instr, label);
}

int NdbInterpretedCode::branch_lt(Uint32 RegLvalue,
                                  Uint32 RegRvalue,
                                  Uint32 label) {
  if ((RegLvalue >= MaxReg) ||
      (RegRvalue >= MaxReg))
    return error(BadRegister);
  Uint32 instr = Interpreter::Branch(Interpreter::BRANCH_LT_REG_REG,
                                     RegRvalue,
                                     RegLvalue);
  return add_branch(instr, label);
}

int NdbInterpretedCode::branch_eq(Uint32 RegLvalue,
                                  Uint32 RegRvalue,
                                  Uint32 label) {
  if ((RegLvalue >= MaxReg) ||
      (RegRvalue >= MaxReg))
    return error(BadRegister);
  Uint32 instr = Interpreter::Branch(Interpreter::BRANCH_EQ_REG_REG,
                                     RegLvalue,
                                     RegRvalue);
  return add_branch(instr, label);
}

int NdbInterpretedCode::branch_ne(Uint32 RegLvalue,
                                  Uint32 RegRvalue,
                                  Uint32 label) {
  if ((RegLvalue >= MaxReg) ||
      (RegRvalue >= MaxReg))
    return error(BadRegister);
  Uint32 instr = Interpreter::Branch(Interpreter::BRANCH_NE_REG_REG,
                                     RegLvalue,
                                     RegRvalue);
  return add_branch(instr, label);
}

int NdbInterpretedCode::branch_ge_const(Uint32 RegLvalue,
                                        Uint16 Constant,
                                        Uint32 label) {
  if (RegLvalue >= MaxReg)
    return error(BadRegister);
  if (Constant >= MaxBranchConst)
    return error(BadConstant);
  Uint32 instr = Interpreter::BranchConstant(Interpreter::BRANCH_GE_REG_REG,
                                             RegLvalue,
                                             Constant);
  return add_branch(instr, label);
}

int NdbInterpretedCode::branch_gt_const(Uint32 RegLvalue,
                                        Uint16 Constant,
                                        Uint32 label) {
  if (RegLvalue >= MaxReg)
    return error(BadRegister);
  if (Constant >= MaxBranchConst)
    return error(BadConstant);
  Uint32 instr = Interpreter::BranchConstant(Interpreter::BRANCH_GT_REG_REG,
                                             RegLvalue,
                                             Constant);
  return add_branch(instr, label);
}

int NdbInterpretedCode::branch_le_const(Uint32 RegLvalue,
                                        Uint16 Constant,
                                        Uint32 label) {
  if (RegLvalue >= MaxReg)
    return error(BadRegister);
  if (Constant >= MaxBranchConst)
    return error(BadConstant);
  Uint32 instr = Interpreter::BranchConstant(Interpreter::BRANCH_LE_REG_REG,
                                             RegLvalue,
                                             Constant);
  return add_branch(instr, label);
}

int NdbInterpretedCode::branch_lt_const(Uint32 RegLvalue,
                                        Uint16 Constant,
                                        Uint32 label) {
  if (RegLvalue >= MaxReg)
    return error(BadRegister);
  if (Constant >= MaxBranchConst)
    return error(BadConstant);
  Uint32 instr = Interpreter::BranchConstant(Interpreter::BRANCH_LT_REG_REG,
                                             RegLvalue,
                                             Constant);
  return add_branch(instr, label);
}

int NdbInterpretedCode::branch_eq_const(Uint32 RegLvalue,
                                        Uint16 Constant,
                                        Uint32 label) {
  if (RegLvalue >= MaxReg)
    return error(BadRegister);
  if (Constant >= MaxBranchConst)
    return error(BadConstant);
  Uint32 instr = Interpreter::BranchConstant(Interpreter::BRANCH_EQ_REG_REG,
                                             RegLvalue,
                                             Constant);
  return add_branch(instr, label);
}

int NdbInterpretedCode::branch_ne_const(Uint32 RegLvalue,
                                        Uint16 Constant,
                                        Uint32 label) {
  if (RegLvalue >= MaxReg)
    return error(BadRegister);
  if (Constant >= MaxBranchConst)
    return error(BadConstant);
  Uint32 instr = Interpreter::BranchConstant(Interpreter::BRANCH_NE_REG_REG,
                                             RegLvalue % MaxReg,
                                             Constant % MaxBranchConst);
  return add_branch(instr, label);
}

int NdbInterpretedCode::branch_ne_null(Uint32 RegLvalue, Uint32 label) {
  if (RegLvalue >= MaxReg)
    return error(BadRegister);
  return add_branch(
      ((RegLvalue) << 6) | Interpreter::BRANCH_REG_NE_NULL, label);
}

int NdbInterpretedCode::branch_eq_null(Uint32 RegLvalue, Uint32 label) {
  if (RegLvalue >= MaxReg)
    return error(BadRegister);
  return add_branch(
      ((RegLvalue) << 6) | Interpreter::BRANCH_REG_EQ_NULL, label);
}

int NdbInterpretedCode::branch_col_eq_null(Uint32 attrId, Uint32 label) {
  int res = 0;

  if (unlikely(m_table_impl == nullptr))
    /* NdbInterpretedCode instruction requires that table is set */
    return error(4538);
  const NdbColumnImpl *c = m_table_impl->getColumn(attrId);

  if (unlikely(c == nullptr)) return error(BadAttributeId);

  if (c->m_storageType == NDB_STORAGETYPE_DISK) m_flags |= UsesDisk;

  /* Add instruction and branch label */
  if ((res = add_branch(Interpreter::BRANCH_ATTR_EQ_NULL, label)) != 0)
    return res;

  /* Add attrId with no length */
  return add1(Interpreter::BranchCol_2(attrId));
}

int NdbInterpretedCode::branch_col_ne_null(Uint32 attrId, Uint32 label) {
  int res = 0;

  if (unlikely(m_table_impl == nullptr))
    /* NdbInterpretedCode instruction requires that table is set */
    return error(4538);
  const NdbColumnImpl *c = m_table_impl->getColumn(attrId);

  if (unlikely(c == nullptr)) return error(BadAttributeId);

  if (c->m_storageType == NDB_STORAGETYPE_DISK) m_flags |= UsesDisk;

  /* Add instruction and branch label */
  if ((res = add_branch(Interpreter::BRANCH_ATTR_NE_NULL, label)) != 0)
    return res;

  /* Add attrId with no length */
  return add1(Interpreter::BranchCol_2(attrId));
}

int NdbInterpretedCode::branch_col_val(Uint32 branch_type, Uint32 attrId,
                                       const void *val, Uint32 len,
                                       Uint32 label) {
  DBUG_ENTER("NdbInterpretedCode::branch_col_val");
  DBUG_PRINT("enter", ("type: %u  col:%u  val: %p  len: %u  label: %u",
                       branch_type, attrId, val, len, label));
  if (val != nullptr)
    DBUG_DUMP("value", (const uchar *)val, len);
  else
    DBUG_PRINT("info", ("value == NULL"));

  Interpreter::BinaryCondition cond = (Interpreter::BinaryCondition)branch_type;

  if (unlikely(m_table_impl == nullptr))
    /* NdbInterpretedCode instruction requires that table is set */
    DBUG_RETURN(error(4538));

  const NdbColumnImpl *col = m_table_impl->getColumn(attrId);

  if (col == nullptr) {
    DBUG_RETURN(error(BadAttributeId));
  }

  Uint32 lastWordMask = ~0;
  if (val == nullptr)
    len = 0;
  else {
    if (!col->getStringType()) {
      /* Fixed size type */
      if (col->getType() == NDB_TYPE_BIT) {
        /* We want to zero out insignificant bits in the
         * last word of a bit type
         */
        Uint32 bitLen = col->getLength();
        Uint32 lastWordBits = bitLen & 0x1F;
        if (lastWordBits) lastWordMask = ((Uint32)1 << lastWordBits) - 1;
      }
      len = col->m_attrSize * col->m_arraySize;
    } else {
      /* For Like and Not like we must use the passed in
       * length.  Otherwise we use the length encoded
       * in the passed string
       */
      if ((branch_type != Interpreter::LIKE) &&
          (branch_type != Interpreter::NOT_LIKE)) {
        if (!col->get_var_length(val, len)) {
          DBUG_RETURN(error(BadLength));
        }
      }
    }
  }

  if (col->m_storageType == NDB_STORAGETYPE_DISK) m_flags |= UsesDisk;

  Interpreter::NullSemantics nulls = Interpreter::NULL_CMP_EQUAL;
  if (m_unknown_action == CmpHasNoUnknowns)
    nulls = Interpreter::NULL_CMP_EQUAL;
  else if (m_unknown_action == BranchIfUnknown)
    nulls = Interpreter::IF_NULL_BREAK_OUT;
  else  // ContinueIfUnknown
    nulls = Interpreter::IF_NULL_CONTINUE;

  if (add_branch(Interpreter::BranchCol(cond, nulls), label) != 0)
    DBUG_RETURN(-1);

  if (add1(Interpreter::BranchCol_2(attrId, len)) != 0) DBUG_RETURN(-1);

  /* Get value byte length rounded up to nearest 32-bit word */
  Uint32 len2 = Interpreter::mod4(len);
  if ((len2 == len) && (lastWordMask == (Uint32)~0)) {
    /* Whole number of 32-bit words */
    DBUG_RETURN(addN((const Uint32 *)val, len2 >> 2));
  }

  /* else */
  /* Partial last word */
  len2 -= 4;
  if (addN((const Uint32 *)val, len2 >> 2) != 0) DBUG_RETURN(-1);

  /* Zero insignificant bytes in last word */
  Uint32 tmp = 0;
  for (Uint32 i = 0; i < len - len2; i++) {
    char *p = (char *)&tmp;
    p[i] = ((const char *)val)[len2 + i];
  }
  DBUG_RETURN(add1((tmp & lastWordMask)));
}

int NdbInterpretedCode::branch_col_col(Uint32 branch_type, Uint32 attrId1,
                                       Uint32 attrId2, Uint32 label) {
  DBUG_ENTER("NdbInterpretedCode::branch_col_col");
  DBUG_PRINT("enter", ("type: %u  col1:%u  col2:%u  label: %u", branch_type,
                       attrId1, attrId2, label));
  const Interpreter::BinaryCondition cond =
      (Interpreter::BinaryCondition)branch_type;
  assert(cond != Interpreter::LIKE && cond != Interpreter::NOT_LIKE);

  if (unlikely(m_table_impl == nullptr))
    /* NdbInterpretedCode instruction requires that table is set */
    DBUG_RETURN(error(4538));

  const NdbColumnImpl *col1 = m_table_impl->getColumn(attrId1);
  const NdbColumnImpl *col2 = m_table_impl->getColumn(attrId2);

  if (unlikely(col1 == nullptr || col2 == nullptr))
    DBUG_RETURN(error(BadAttributeId));

  if (unlikely(col1->isBindable(*col2) != 0)) {
    /* A bindable column is of same type, prec, length, scale and cs.
     * Blob and Text columns are also excluded as not bindable.
     */
    DBUG_RETURN(error(4557));
  }
  if (col1->m_storageType == NDB_STORAGETYPE_DISK ||
      col2->m_storageType == NDB_STORAGETYPE_DISK)
    m_flags |= UsesDisk;

  Interpreter::NullSemantics nulls = Interpreter::NULL_CMP_EQUAL;
  if (m_unknown_action == CmpHasNoUnknowns)
    nulls = Interpreter::NULL_CMP_EQUAL;
  else if (m_unknown_action == BranchIfUnknown)
    nulls = Interpreter::IF_NULL_BREAK_OUT;
  else  // ContinueIfUnknown
    nulls = Interpreter::IF_NULL_CONTINUE;

  if (add_branch(Interpreter::BranchColAttrId(cond, nulls), label) != 0)
    DBUG_RETURN(-1);

  if (add1(Interpreter::BranchColAttrId_2(attrId1, attrId2)) != 0)
    DBUG_RETURN(-1);

  DBUG_RETURN(0);
}

int NdbInterpretedCode::branch_col_param(Uint32 branch_type, Uint32 attrId,
                                         Uint32 paramId, Uint32 label) {
  DBUG_ENTER("NdbInterpretedCode::branch_col_param");
  DBUG_PRINT("enter", ("type: %u  col:%u  param:%u  label: %u", branch_type,
                       attrId, paramId, label));
  const Interpreter::BinaryCondition cond =
      (Interpreter::BinaryCondition)branch_type;
  assert(cond != Interpreter::LIKE && cond != Interpreter::NOT_LIKE);

  if (unlikely(m_table_impl == nullptr))
    /* NdbInterpretedCode instruction requires that table is set */
    DBUG_RETURN(error(4538));

  const NdbColumnImpl *col = m_table_impl->getColumn(attrId);
  if (unlikely(col == nullptr)) DBUG_RETURN(error(BadAttributeId));

  if (col->m_storageType == NDB_STORAGETYPE_DISK) m_flags |= UsesDisk;

  Interpreter::NullSemantics nulls = Interpreter::NULL_CMP_EQUAL;
  if (m_unknown_action == CmpHasNoUnknowns)
    nulls = Interpreter::NULL_CMP_EQUAL;
  else if (m_unknown_action == BranchIfUnknown)
    nulls = Interpreter::IF_NULL_BREAK_OUT;
  else  // ContinueIfUnknown
    nulls = Interpreter::IF_NULL_CONTINUE;

  if (add_branch(Interpreter::BranchColParameter(cond, nulls), label) != 0)
    DBUG_RETURN(-1);

  if (add1(Interpreter::BranchColParameter_2(attrId, paramId)) != 0)
    DBUG_RETURN(-1);

  DBUG_RETURN(0);
}

int NdbInterpretedCode::branch_col_eq(const void *val, Uint32, Uint32 attrId,
                                      Uint32 label) {
  return branch_col_val(Interpreter::EQ, attrId, val, 0, label);
}

int NdbInterpretedCode::branch_col_ne(const void *val, Uint32, Uint32 attrId,
                                      Uint32 label) {
  return branch_col_val(Interpreter::NE, attrId, val, 0, label);
}

int NdbInterpretedCode::branch_col_lt(const void *val, Uint32, Uint32 attrId,
                                      Uint32 label) {
  return branch_col_val(Interpreter::LT, attrId, val, 0, label);
}

int NdbInterpretedCode::branch_col_le(const void *val, Uint32, Uint32 attrId,
                                      Uint32 label) {
  return branch_col_val(Interpreter::LE, attrId, val, 0, label);
}

int NdbInterpretedCode::branch_col_gt(const void *val, Uint32, Uint32 attrId,
                                      Uint32 label) {
  return branch_col_val(Interpreter::GT, attrId, val, 0, label);
}

int NdbInterpretedCode::branch_col_ge(const void *val, Uint32, Uint32 attrId,
                                      Uint32 label) {
  return branch_col_val(Interpreter::GE, attrId, val, 0, label);
}

int NdbInterpretedCode::branch_col_like(const void *val, Uint32 len,
                                        Uint32 attrId, Uint32 label) {
  return branch_col_val(Interpreter::LIKE, attrId, val, len, label);
}

int NdbInterpretedCode::branch_col_notlike(const void *val, Uint32 len,
                                           Uint32 attrId, Uint32 label) {
  return branch_col_val(Interpreter::NOT_LIKE, attrId, val, len, label);
}

int NdbInterpretedCode::branch_col_and_mask_eq_mask(const void *mask, Uint32,
                                                    Uint32 attrId,
                                                    Uint32 label) {
  return branch_col_val(Interpreter::AND_EQ_MASK, attrId, mask, 0, label);
}

int NdbInterpretedCode::branch_col_and_mask_ne_mask(const void *mask, Uint32,
                                                    Uint32 attrId,
                                                    Uint32 label) {
  return branch_col_val(Interpreter::AND_NE_MASK, attrId, mask, 0, label);
}

int NdbInterpretedCode::branch_col_and_mask_eq_zero(const void *mask, Uint32,
                                                    Uint32 attrId,
                                                    Uint32 label) {
  return branch_col_val(Interpreter::AND_EQ_ZERO, attrId, mask, 0, label);
}

int NdbInterpretedCode::branch_col_and_mask_ne_zero(const void *mask, Uint32,
                                                    Uint32 attrId,
                                                    Uint32 label) {
  return branch_col_val(Interpreter::AND_NE_ZERO, attrId, mask, 0, label);
}

/**
 * Variants taking two attr arguments.
 */
int NdbInterpretedCode::branch_col_eq(Uint32 attrId1, Uint32 attrId2,
                                      Uint32 label) {
  return branch_col_col(Interpreter::EQ, attrId1, attrId2, label);
}

int NdbInterpretedCode::branch_col_ne(Uint32 attrId1, Uint32 attrId2,
                                      Uint32 label) {
  return branch_col_col(Interpreter::NE, attrId1, attrId2, label);
}

int NdbInterpretedCode::branch_col_lt(Uint32 attrId1, Uint32 attrId2,
                                      Uint32 label) {
  return branch_col_col(Interpreter::LT, attrId1, attrId2, label);
}

int NdbInterpretedCode::branch_col_le(Uint32 attrId1, Uint32 attrId2,
                                      Uint32 label) {
  return branch_col_col(Interpreter::LE, attrId1, attrId2, label);
}

int NdbInterpretedCode::branch_col_gt(Uint32 attrId1, Uint32 attrId2,
                                      Uint32 label) {
  return branch_col_col(Interpreter::GT, attrId1, attrId2, label);
}

int NdbInterpretedCode::branch_col_ge(Uint32 attrId1, Uint32 attrId2,
                                      Uint32 label) {
  return branch_col_col(Interpreter::GE, attrId1, attrId2, label);
}

/**
 * Variants taking an attrId and a paramId arguments.
 */
int NdbInterpretedCode::branch_col_eq_param(Uint32 attrId, Uint32 paramId,
                                            Uint32 label) {
  return branch_col_param(Interpreter::EQ, attrId, paramId, label);
}

int NdbInterpretedCode::branch_col_ne_param(Uint32 attrId, Uint32 paramId,
                                            Uint32 label) {
  return branch_col_param(Interpreter::NE, attrId, paramId, label);
}

int NdbInterpretedCode::branch_col_lt_param(Uint32 attrId, Uint32 paramId,
                                            Uint32 label) {
  return branch_col_param(Interpreter::LT, attrId, paramId, label);
}

int NdbInterpretedCode::branch_col_le_param(Uint32 attrId, Uint32 paramId,
                                            Uint32 label) {
  return branch_col_param(Interpreter::LE, attrId, paramId, label);
}

int NdbInterpretedCode::branch_col_gt_param(Uint32 attrId, Uint32 paramId,
                                            Uint32 label) {
  return branch_col_param(Interpreter::GT, attrId, paramId, label);
}

int NdbInterpretedCode::branch_col_ge_param(Uint32 attrId, Uint32 paramId,
                                            Uint32 label) {
  return branch_col_param(Interpreter::GE, attrId, paramId, label);
}

int NdbInterpretedCode::interpret_exit_ok() {
  return add1(Interpreter::EXIT_OK);
}

int NdbInterpretedCode::interpret_exit_nok(Uint32 ErrorCode) {
  return add1((ErrorCode << 16) | Interpreter::EXIT_REFUSE);
}

int NdbInterpretedCode::interpret_exit_nok() {
  return add1((626 << 16) | Interpreter::EXIT_REFUSE);
}

int NdbInterpretedCode::interpret_exit_last_row() {
  return add1(Interpreter::EXIT_OK_LAST);
}

int NdbInterpretedCode::add_val(Uint32 attrId, Uint32 aValue) {
  /* Load attribute into register 6 */
  int res = 0;
  if ((res = read_attr(6, attrId) != 0)) return res;

  /* Load constant into register 7 */
  /* We attempt to use the smallest constant load
   * instruction
   */
  if (aValue < (1 << 16)) {
    if ((res = load_const_u16(7, aValue)) != 0) return res;
  } else {
    if ((res = load_const_u32(7, aValue)) != 0) return res;
  }

  /* Add registers 6 and 7 -> 7*/
  if ((res = add_reg(7, 6, 7)) != 0) return res;

  /* Write back */
  return write_attr(attrId, 7);
}

int NdbInterpretedCode::add_val(Uint32 attrId, Uint64 aValue) {
  /* Load attribute into register 6 */
  int res = 0;
  if ((res = read_attr(6, attrId) != 0)) return res;

  /* Load constant into register 7 */
  /* We attempt to use the smallest constant load
   * instruction
   */
  if ((aValue >> 32) == 0) {
    if (aValue < (1 << 16)) {
      if ((res = load_const_u16(7, (Uint32)aValue)) != 0) return res;
    } else {
      if ((res = load_const_u32(7, (Uint32)aValue)) != 0) return res;
    }
  } else if ((res = load_const_u64(7, aValue)) != 0)
    return res;

  /* Add registers 6 and 7 -> 7*/
  if ((res = add_reg(7, 6, 7)) != 0) return res;

  /* Write back */
  return write_attr(attrId, 7);
}

int NdbInterpretedCode::sub_val(Uint32 attrId, Uint32 aValue) {
  /* Load attribute into register 6 */
  int res = 0;
  if ((res = read_attr(6, attrId) != 0)) return res;

  /* Load constant into register 7 */
  /* We attempt to use the smallest constant load
   * instruction
   */
  if (aValue < (1 << 16)) {
    if ((res = load_const_u16(7, aValue)) != 0) return res;
  } else {
    if ((res = load_const_u32(7, aValue)) != 0) return res;
  }

  /* Subtract register (R7=R6-R7)*/
  if ((res = sub_reg(7, 6, 7)) != 0) return res;

  /* Write back */
  return write_attr(attrId, 7);
}

int NdbInterpretedCode::sub_val(Uint32 attrId, Uint64 aValue) {
  /* Load attribute into register 6 */
  int res = 0;
  if ((res = read_attr(6, attrId) != 0)) return res;

  /* Load constant into register 7 */
  /* We attempt to use the smallest constant load
   * instruction
   */
  if ((aValue >> 32) == 0) {
    if (aValue < (1 << 16)) {
      if ((res = load_const_u16(7, (Uint32)aValue)) != 0) return res;
    } else {
      if ((res = load_const_u32(7, (Uint32)aValue)) != 0) return res;
    }
  } else {
    if ((res = load_const_u64(7, aValue)) != 0) return res;
  }

  /* Subtract register (R7=R6-R7)*/
  if ((res = sub_reg(7, 6, 7)) != 0) return res;

  /* Write back */
  return write_attr(attrId, 7);
}

int NdbInterpretedCode::def_sub(Uint32 SubroutineNumber) {
  if (SubroutineNumber > MaxSubs) return error(BadSubNumber);

  if (m_flags & InSubroutineDef) return error(BadState);

  if (m_number_of_calls == 0) return error(BadState);

  /* Record where subroutines start */
  if (m_number_of_subs == 0)
    m_first_sub_instruction_pos = m_instructions_length;

  m_number_of_subs++;
  m_flags |= InSubroutineDef;

  CodeMetaInfo info;

  info.type = Subroutine;
  info.number = SubroutineNumber;
  info.firstInstrPos = m_instructions_length - m_first_sub_instruction_pos;

  // Note, no check for whether the label's already defined here.
  return addMeta(info);
}

int NdbInterpretedCode::call_sub(Uint32 SubroutineNumber) {
  if (SubroutineNumber > MaxSubs) return error(BadState);

  m_number_of_calls++;

  return add1(Interpreter::CALL | (SubroutineNumber << 16));
}

int NdbInterpretedCode::ret_sub() {
  if ((m_flags & InSubroutineDef) == 0) return error(BadState);

  m_flags &= ~(InSubroutineDef);

  return add1(Interpreter::RETURN);
}

/* Get a CodeMetaInfo object given a number
 * Label numbers start from 0.  Subroutine numbers start from
 * the highest label number
 */
int NdbInterpretedCode::getInfo(Uint32 number, CodeMetaInfo &info) const {
  if (number >= (m_number_of_labels + m_number_of_subs)) return -1;

  Uint32 pos = m_buffer_length - ((number + 1) * CODEMETAINFO_WORDS);

  info.number = (m_buffer[pos + 1] >> 16) & 0xffff;
  info.type = m_buffer[pos + 1] & 0xffff;
  info.firstInstrPos = m_buffer[pos];

  return 0;
}

/* Update internal NdbError object based on its code */
static void update(NdbError &error) {
  ndberror_struct ndberror = (ndberror_struct)error;
  ndberror_update(&ndberror);
  error = NdbError(ndberror);
}

const NdbDictionary::Table *NdbInterpretedCode::getTable() const {
  return (m_table_impl == nullptr) ? nullptr : m_table_impl->m_facade;
}

const NdbError &NdbInterpretedCode::getNdbError() const {
  /* Set the correct error info before returning to
   * caller
   */
  update(m_error);
  return m_error;
}

Uint32 NdbInterpretedCode::getWordsUsed() const {
  return (m_buffer_length - m_available_length);
}

int NdbInterpretedCode::copy(const NdbInterpretedCode &src) {
  m_table_impl = src.m_table_impl;
  m_buffer_length = src.m_buffer_length;

  /**
   * Each NdbInterpretedCode manages life cycle of m_internal_buffer.
   */
  if (m_internal_buffer != nullptr) {
    delete[] m_internal_buffer;
    m_internal_buffer = nullptr;
  }

  if (src.m_internal_buffer == nullptr) {
    // External buffer with externally managed life cycle.
    m_buffer = src.m_buffer;
  } else {
    m_buffer = m_internal_buffer = new Uint32[m_buffer_length];
    if (unlikely(m_internal_buffer == nullptr)) {
      return 4000;  // Allocation failed.
    }
    memcpy(m_internal_buffer, src.m_internal_buffer,
           m_buffer_length * sizeof(Uint32));
  }

  m_number_of_labels = src.m_number_of_labels;
  m_number_of_subs = src.m_number_of_subs;
  m_number_of_calls = src.m_number_of_calls;
  m_last_meta_pos = src.m_last_meta_pos;
  m_instructions_length = src.m_instructions_length;
  m_first_sub_instruction_pos = src.m_first_sub_instruction_pos;
  m_available_length = src.m_available_length;
  m_flags = src.m_flags;
  m_error = src.m_error;
  return 0;
}

/* CodeMetaInfo comparator for qsort
 * Sort order is highest numbered sub to lowest,
 * then highest numbered label to lowest
 * *va < *vb  : -1  *va first
 * *va == *vb : 0
 * *va > *vb  : 1   *vb first
 */
int NdbInterpretedCode::compareMetaInfo(const void *va, const void *vb) {
  Uint32 aWord = *(((const Uint32 *)va) + 1);  // number || type
  Uint32 bWord = *(((const Uint32 *)vb) + 1);  // number || type
  Uint16 aType = aWord & 0xffff;
  Uint16 bType = bWord & 0xffff;
  const int AFirst = -1;
  const int BFirst = 1;

  /* Sort in order (Subs, Labels) */
  if (aType != bType) return (aType == Subroutine) ? AFirst : BFirst;

  Uint16 aNumber = (aWord >> 16) & 0xffff;
  Uint16 bNumber = (bWord >> 16) & 0xffff;

  /* Sort in reverse order within type, highest number
   * first.
   */
  if (aNumber != bNumber) return (bNumber > aNumber) ? BFirst : AFirst;

  return 0;  // Should never happen
}

int NdbInterpretedCode::finalise() {
  if (m_instructions_length == 0) {
    /* We will attempt to add a single EXIT_OK instruction
     * rather than returning an error.
     * This may simplify client code.
     */
    int res = 0;
    if (0 != (res = interpret_exit_ok())) return -1;
  }

  assert(m_buffer != nullptr);

  /* Use label and subroutine meta-info at
   * the end of the code buffer to determine
   * the correct offsets for label branches and
   * subroutine calls
   */
  Uint32 numOfMetaInfos = m_number_of_labels + m_number_of_subs;
  Uint32 sizeOfMetaInfo = numOfMetaInfos * CODEMETAINFO_WORDS;
  Uint32 startOfMetaInfo = m_buffer_length - sizeOfMetaInfo;

  /* Sort different types of meta info into order in place */
  qsort(&m_buffer[startOfMetaInfo], numOfMetaInfos, CODEMETAINFO_WORDS << 2,
        &compareMetaInfo);

  /* Loop over instructions, patching up branches
   * and calls
   */
  Uint32 *ip = m_buffer;
  Uint32 *nextIp = ip;
  Uint32 const *firstInstruction = m_buffer;
  Uint32 const *endOfProgram = m_buffer + m_instructions_length;

  while (ip < endOfProgram) {
    Interpreter::InstructionPreProcessing action;
    nextIp = Interpreter::getInstructionPreProcessingInfo(ip, action);
    if (nextIp == nullptr) {
      m_error.code = 4516;  // Illegal instruction in interpreted program
      return -1;
    }

    switch (action) {
      case Interpreter::NONE:
        /* Normal instruction, skip over */
        break;
      case Interpreter::LABEL_ADDRESS_REPLACEMENT: {
        /* Have a branch needing a relative label address replacement */
        Uint32 label = Interpreter::getLabel(*ip);

        if (label > m_number_of_labels) {
          m_error.code = 4517;  // Bad label in branch instruction
          return -1;
        }

        CodeMetaInfo info;
        if (getInfo(label, info) != 0) {
          m_error.code = 4222;  // Label was not found, internal error
          return -1;
        }

        assert(info.type == Label);

        Uint32 currOffset = Uint32(ip - firstInstruction);
        Uint32 labelOffset = info.firstInstrPos;

        if (labelOffset >= m_instructions_length) {
          m_error.code= 4565; // Bad label in branch instruction
          return -1;
        }

        /* Remove the label info */
        Uint32 patchedInstruction = (*ip) & 0xffff;

        if (labelOffset < currOffset) /* Backwards branch */
          patchedInstruction |=
              (((currOffset - labelOffset) << 16) | ((Uint32)1 << 31));
        else
          /* Forwards branch */
          patchedInstruction |= ((labelOffset - currOffset) << 16);

        *ip = patchedInstruction;
        break;
      }
      case Interpreter::SUB_ADDRESS_REPLACEMENT: {
        /* Have a call to a subtoutine that needs to become
         * an offset within the subroutines section
         */
        Uint32 subroutine = Interpreter::getLabel(*ip);

        if (subroutine > m_number_of_subs) {
          m_error.code = 4520;  // Call to undefined subroutine
          return -1;
        }

        CodeMetaInfo info;
        if (getInfo(m_number_of_labels + subroutine, info) != 0) {
          m_error.code = 4521;  // Call to undefined subroutine, internal error
          return -1;
        }

        assert(info.type == Subroutine);

        Uint32 subOffset = info.firstInstrPos;

        if (subOffset > (m_instructions_length - m_first_sub_instruction_pos)) {
          m_error.code = 4521;  // Call to undefined subroutine, internal error
          return -1;
        }

        /* Replace the label in the call with the subroutine
         * offset
         */
        Uint32 patchedInstruction = (*ip) & 0xffff;
        patchedInstruction |= subOffset << 16;
        *ip = patchedInstruction;

        break;
      }
      default:
        m_error.code = 4516;  // Illegal instruction in interpreted program
        return -1;
    }

    ip = nextIp;
  }

  /* Code has been patched-up */
  m_flags |= Finalised;

  return 0;
}
