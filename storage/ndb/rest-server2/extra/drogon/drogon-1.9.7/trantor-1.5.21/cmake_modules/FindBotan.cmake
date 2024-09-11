function(find_botan_pkgconfig package_name botan_ver)
  if(TARGET Botan::Botan)
    return()
  endif()

  pkg_check_modules(
    Botan
    QUIET
    IMPORTED_TARGET
    ${package_name}
  )
  if(TARGET PkgConfig::Botan)
    add_library(Botan::Botan ALIAS PkgConfig::Botan)

    if(botan_ver EQUAL 3)
      target_compile_features(PkgConfig::Botan INTERFACE cxx_std_20)
    endif()
  endif()
endfunction()

function(find_botan_search package_name botan_ver)
  if(TARGET Botan::Botan)
    return()
  endif()
  find_path(
    Botan_INCLUDE_DIRS
    NAMES botan/botan.h
    PATH_SUFFIXES ${package_name}
    DOC "The Botan include directory"
  )

  find_library(
    Botan_LIBRARIES
    NAMES botan ${package_name}
    DOC "The Botan library"
  )

  mark_as_advanced(Botan_INCLUDE_DIRS Botan_LIBRARIES)

  add_library(Botan::Botan IMPORTED UNKNOWN)
  set_target_properties(
    Botan::Botan
    PROPERTIES
    IMPORTED_LOCATION "${Botan_LIBRARIES}"
    INTERFACE_INCLUDE_DIRECTORIES "${Botan_INCLUDE_DIRS}"
  )
  if(botan_ver EQUAL 3)
    target_compile_features(Botan::Botan INTERFACE cxx_std_20)
  endif()

  if(WIN32)
    target_compile_definitions(Botan::Botan INTERFACE -DNOMINMAX=1)
  endif()
endfunction()

find_package(PkgConfig)
if(NOT WIN32 AND PKG_CONFIG_FOUND)
  # find_botan_pkgconfig(botan-2 2)
  find_botan_pkgconfig(botan-3 3)
endif()

if(NOT TARGET Botan::Botan)
  # find_botan_search(botan-2 2)
  find_botan_search(botan-3 3)
endif()

include(FindPackageHandleStandardArgs)
find_package_handle_standard_args(Botan REQUIRED_VARS Botan_LIBRARIES Botan_INCLUDE_DIRS)
