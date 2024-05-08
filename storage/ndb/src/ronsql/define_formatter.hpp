/*
   Copyright (c) 2024, 2024, Hopsworks and/or its affiliates.

   This program is free software; you can redistribute it and/or modify
   it under the terms of the GNU General Public License, version 2.0,
   as published by the Free Software Foundation.

   This program is also distributed with certain software (including
   but not limited to OpenSSL) that is licensed under separate terms,
   as designated in a particular file or component or in included license
   documentation.  The authors of MySQL hereby grant you an additional
   permission to link the program and your derivative works with the
   separately licensed software that they have included with MySQL.

   This program is distributed in the hope that it will be useful,
   but WITHOUT ANY WARRANTY; without even the implied warranty of
   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
   GNU General Public License, version 2.0, for more details.

   You should have received a copy of the GNU General Public License
   along with this program; if not, write to the Free Software
   Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA 02110-1301  USA
*/

#ifndef define_formatter_hpp_included
#define define_formatter_hpp_included 1

/*
 * The DEFINE_FORMATTER macro is used to define a custom formatter for
 * convenient usage with ostream, but without std::format which was introduced
 * in C++20. The anonymous namespace limits the formatter's visibility to the
 * current translation unit, i.e. it will behave similarly to a function
 * declared `static` and so won't be visible to the linker. The macro parameters
 * are:
 * - A name for the formatter. This is technically a class name although it's
 *   supposed to feel like a function when it's used.
 * - The type of the value, the thing to be formatted. Note that const
 *   qualifier(s) will be added automatically.
 * - A body of code printing `value` to `os`. Enclosing it in braces can provide
 *   clarity but is not required.
 * It is possible to define several formatters with the same name, as long as
 * the value type is different enough.
 *
 * Example usage:
 ***********************************************************
     #include <iostream>
     #include <cstring>
     #include "define_formatter.hpp"

     DEFINE_FORMATTER(pad2, uint, {
       if (value < 10) os << '0';
       os << value;
     })

     DEFINE_FORMATTER(pad4, uint, {
       if (value < 10) os << '0';
       if (value < 100) os << '0';
       if (value < 1000) os << '0';
       os << value;
     })

     DEFINE_FORMATTER(pad4, char*, {
       for (uint i = strlen(value); i < 4; i++)
         os << '_';
       os << value;
     })

     int main()
     {
       std::cout << "Padded 2: " << pad2(1) << '\n'
                 << "Padded 4: " << pad4(23) << '\n'
                 << "Padded 4: " << pad4("abc") << '\n';
     }
 ***********************************************************
 *
 */


// DEFINE_FORMATTER__addconst_t<some_type> is a helper type. It adds the const
// qualifier to some_type at all levels of indirection. For example:
//   DEFINE_FORMATTER__addconst_t<int> -> const int
//   DEFINE_FORMATTER__addconst_t<int**> -> const int *const *const
// This is helpful, since operator<< needs a const argument.
template<typename T>struct DEFINE_FORMATTER__addconst
{ using type = const T; };
template<typename T>struct DEFINE_FORMATTER__addconst<T*>
{ using type = typename DEFINE_FORMATTER__addconst<T>::type* const; };
template<typename T>using  DEFINE_FORMATTER__addconst_t =
  typename DEFINE_FORMATTER__addconst<T>::type;

// We want to be able to define formatters with the same name for different
// value types, and formatters with different names for the same value type, all
// with distinct logic. Function overloading works well for this, however there
// is one issue: A formatter is essentially an overload for operator<<, which
// will be dispatched/selected based on the type returned by the formatter.
// Therefore, the return type of each formatter, i.e. combination of formatter
// name and value type, has to be distinct. We define a wrapper class for this purpose. We need a unique name for each such
// class. DEFINE_FORMATTER__GENSYM is a helper macro that generates a unique
// identifier given a prefix. It needs two levels of indirection in order to
// correctly expand before concatenating. For example:
//   DEFINE_FORMATTER__GENSYM(hello)
//   => DEFINE_FORMATTER__CONCAT1(hello, __COUNTER__)
//   => DEFINE_FORMATTER__CONCAT2(hello, 7)
//   => hello7
#define DEFINE_FORMATTER__GENSYM(X) DEFINE_FORMATTER__CONCAT1(X, __COUNTER__)
#define DEFINE_FORMATTER__CONCAT1(x, y) DEFINE_FORMATTER__CONCAT2(x, y)
#define DEFINE_FORMATTER__CONCAT2(x, y) x##y

// DEFINE_FORMATTER is the user interface as described above. The helper macro
// call allows us to
// - Add the const qualifiers to the value type in just one place.
// - Generate a unique name for a wrapper class that can then be used in several
//   places. More than one call to DEFINE_FORMATTER__GENSYM would result in
//   different names.
// The use of variadic macro arguments is required in order to correctly process
// a code body containing comma tokens.
#define DEFINE_FORMATTER(FORMATTER, TYPE, ...) \
  DEFINE_FORMATTER__DEFS(FORMATTER, \
                         DEFINE_FORMATTER__addconst_t<TYPE>, \
                         DEFINE_FORMATTER__GENSYM(FORMATTER##_wrapper_), \
                         __VA_ARGS__)

// This makes the actual definitions.
// - An anonymous namespace, which limits the visibility of all the definitions
//   to the current translation unit, i.e. it will behave similarly to a
//   function declared `static` and so won't be visible to the linker.
// - A wrapper class with a unique name. This is necessary since operator<<
//   can only dispatch on a type, while we want to dispatch on a combination of
//   identifier and type, like overloaded functions (see comment above).
// - The overloaded operator<< containing the formatting logic.
// - The formatter function itself, whose sole purpose is to achieve the
//   dispatch we want; identifier and type. It makes the formatter work like an
//   overloaded function because that's exactly what it is.
#define DEFINE_FORMATTER__DEFS(FORMATTER, TYPE, WRAPPER, ...) \
  namespace \
  { \
    class WRAPPER { \
    private: \
      TYPE m_value; \
    public: \
      explicit WRAPPER(TYPE value): m_value(value) {} \
      friend inline std::ostream& \
      operator<< (std::ostream& os, const WRAPPER& wrapper_instance) \
      { \
        TYPE& value = wrapper_instance.m_value; \
        __VA_ARGS__ \
        return os; \
      } \
    }; \
    inline WRAPPER FORMATTER(TYPE& value) \
    { \
     return WRAPPER(value); \
    } \
  }

#endif
