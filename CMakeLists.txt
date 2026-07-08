cmake_minimum_required(VERSION 3.28)
project(uFilterPickerPickBroker VERSION 0.0.2 LANGUAGES CXX)
enable_testing()
include(FetchContent)

option(BUILD_SHARED_LIBS "Build using shared libraries" OFF)
option(BUILD_TESTS "Compile unit tests" ON)
option(WITH_CONAN "Build using Conan" OFF)
option(BUILD_CONTAINER "Build containerized application" OFF)
option(USE_CLANG_TIDY "Build using clang-tidy" OFF)
include(CheckCXXCompilerFlag)
include(GenerateExportHeader)

list(APPEND CMAKE_MODULE_PATH ${CMAKE_SOURCE_DIR}/cmake)
set(CMAKE_THREAD_PREFER_PTHREAD TRUE)

# Boost
if(POLICY CMP0167)
  cmake_policy(SET CMP0167 NEW)
endif()
if (NOT BUILD_SHARED_LIBS)
   message("Will attempt to use Boost static libraries")
   set(Boost_USE_STATIC_LIBS ON)
endif()

find_package(Protobuf CONFIG REQUIRED)
find_package(gRPC CONFIG REQUIRED)
if (${WITH_CONAN})
   message("Will use Conan OTLP exporters")
   find_package(opentelemetry-cpp CONFIG COMPONENTS exporter_otlp_grpc exporters_otlp_http REQUIRED)
else()
   find_package(opentelemetry-cpp CONFIG COMPONENTS exporters_otlp_http REQUIRED)
endif()
find_package(spdlog REQUIRED)
find_package(Boost COMPONENTS headers program_options REQUIRED)
find_package(Threads REQUIRED)
#find_package(TBB COMPONENTS tbb REQUIRED)
find_package(SQLite3 REQUIRED)
if(NOT TARGET SQLite3::SQLite3) # CMake < 4.3
  add_library(SQLite3::SQLite3 ALIAS SQLite::SQLite3)
endif()
if (${BUILD_TESTS})
   find_package(Catch2 3 REQUIRED)
endif()

if (${USE_CLANG_TIDY} AND PROJECT_IS_TOP_LEVEL)
   message("Looking for clang-tidy")
   find_program(CLANG_TIDY_EXECUTABLE NAMES "clang-tidy")
   if (CLANG_TIDY_EXECUTABLE)
      message("Using clang-tidy  ${CLANG_TIDY_EXECUTABLE}")
      set(CMAKE_C_CLANG_TIDY   ${CLANG_TIDY_EXECUTABLE} -p ${CMAKE_BINARY_DIR})
      set(CMAKE_CXX_CLANG_TIDY ${CLANG_TIDY_EXECUTABLE} -p ${CMAKE_BINARY_DIR})
      set(CMAKE_EXPORT_COMPILE_COMMANDS ON)
   else()
      message("clang-tidy not found")
   endif()
endif()

find_package(Git)
if (Git_FOUND)
   execute_process(
     COMMAND ${GIT_EXECUTABLE} --no-pager log -n 1 HEAD "--pretty=format:%h"
     OUTPUT_VARIABLE uFilterPickerPickBroker_GITTAG
     RESULT_VARIABLE gitresult
     ERROR_QUIET
     OUTPUT_STRIP_TRAILING_WHITESPACE
   )
   if (gitresult EQUAL 0)
      message(STATUS "Last Git commit: ${uFilterPickerPickBroker_GITTAG}")
   endif()
else()
   set(uFilterPickerPickBroker_GITTAG "")
endif()
configure_file(${CMAKE_SOURCE_DIR}/src/version.hpp.in
               ${CMAKE_SOURCE_DIR}/include/uFilterPickerPickBroker/version.hpp)

set(LIBRARY_SRC
    src/subscribeService.cpp
    src/subscribeServiceOptions.cpp
    src/database.cpp
    src/exception.cpp
    src/publishService.cpp
    src/publishServiceOptions.cpp
    src/grpcServerOptions.cpp
    src/metricsSingleton.cpp
    src/broker.cpp
    src/brokerOptions.cpp
    src/pickStore.cpp
    src/pickStoreOptions.cpp
    src/version.cpp)
set(PROTO_SRC
    uFilterPickerPickBrokerAPI/v1/stream_identifier.proto
    uFilterPickerPickBrokerAPI/v1/phase_hint.proto
    uFilterPickerPickBrokerAPI/v1/pick.proto
    uFilterPickerPickBrokerAPI/v1/algorithm.proto
    uFilterPickerPickBrokerAPI/v1/publish_service.proto
    uFilterPickerPickBrokerAPI/v1/publish_response.proto
    uFilterPickerPickBrokerAPI/v1/subscribe_service.proto
    uFilterPickerPickBrokerAPI/v1/stream_since_request.proto
    )
set(LIBRARY_HEADER_FILES
    include/uFilterPickerPickBroker/subscribeServiceOptions.hpp
    include/uFilterPickerPickBroker/database.hpp
    include/uFilterPickerPickBroker/exception.hpp
    include/uFilterPickerPickBroker/grpcServerOptions.hpp
    include/uFilterPickerPickBroker/publishService.hpp
    include/uFilterPickerPickBroker/publishServiceOptions.hpp
    include/uFilterPickerPickBroker/metricsSingleton.hpp
    include/uFilterPickerPickBroker/brokerOptions.hpp
    include/uFilterPickerPickBroker/pickStore.hpp
    include/uFilterPickerPickBroker/pickStoreOptions.hpp
    include/uFilterPickerPickBroker/version.hpp)

add_library(libuFilterPickerPickBroker STATIC ${LIBRARY_SRC} ${PROTO_SRC})
add_library(uFilterPickerPickBroker::libuFilterPickerPickBroker ALIAS libuFilterPickerPickBroker)
set_target_properties(libuFilterPickerPickBroker PROPERTIES
                      OUTPUT_NAME uFilterPickerPickBroker
                      CXX_STANDARD 23
                      CXX_STANDARD_REQUIRED YES
                      CXX_EXTENSIONS NO)
target_include_directories(libuFilterPickerPickBroker
                           PUBLIC  $<BUILD_INTERFACE:${CMAKE_SOURCE_DIR}/include>
                           PRIVATE $<BUILD_INTERFACE:${CMAKE_SOURCE_DIR}>
                           PRIVATE $<BUILD_INTERFACE:${CMAKE_CURRENT_BINARY_DIR}>
                           PUBLIC  $<INSTALL_INTERFACE:>)
target_link_libraries(libuFilterPickerPickBroker
                      PRIVATE protobuf::libprotobuf
                              spdlog::spdlog_header_only
                              gRPC::grpc gRPC::grpc++
                              SQLite3::SQLite3)
target_sources(libuFilterPickerPickBroker
               PUBLIC FILE_SET HEADERS
                      BASE_DIRS
                         include
                         ${CMAKE_CURRENT_BINARY_DIR}/include
                      FILES
                         ${LIBRARY_HEADER_FILES})

protobuf_generate(TARGET libuFilterPickerPickBroker
                  LANGUAGE cpp
                  OUT_VAR PROTO_GENERATED_CXX_FILES
                 )
protobuf_generate(TARGET libuFilterPickerPickBroker
                  LANGUAGE grpc
                  PLUGIN "protoc-gen-grpc=$<TARGET_FILE:gRPC::grpc_cpp_plugin>"
                  PLUGIN_OPTIONS generate_mock_code=true
                  GENERATE_EXTENSIONS .grpc.pb.h .grpc.pb.cc
                  OUT_VAR PROTO_GENERATED_FILES
                 )
set_source_files_properties(${PROTO_GENERATED_FILES} ${PROTO_GENERATED_CXX_FILES}
   PROPERTIES
      SKIP_LINTING ON
)


add_executable(uFilterPickerPickBroker
               src/main.cpp)
set_target_properties(uFilterPickerPickBroker PROPERTIES
                      CXX_STANDARD 23
                      CXX_STANDARD_REQUIRED YES
                      CXX_EXTENSIONS NO)
target_include_directories(uFilterPickerPickBroker
                           PRIVATE $<BUILD_INTERFACE:${CMAKE_SOURCE_DIR}/include>
                           PRIVATE $<BUILD_INTERFACE:${CMAKE_SOURCE_DIR}/src>
                           PRIVATE $<BUILD_INTERFACE:${CMAKE_CURRENT_BINARY_DIR}>)
if (${WITH_CONAN})
   target_link_libraries(uFilterPickerPickBroker
                         PRIVATE opentelemetry-cpp::opentelemetry-cpp
                                 opentelemetry-cpp::proto_grpc
                                 opentelemetry-cpp::exporter_otlp_grpc
                                 opentelemetry-cpp::exporter_otlp_grpc_client
                                 opentelemetry-cpp::exporter_otlp_grpc_metrics
                                 opentelemetry-cpp::exporter_otlp_http
                                 opentelemetry-cpp::exporter_otlp_http_client
                                 opentelemetry-cpp::exporter_otlp_http_metric
                                 opentelemetry-cpp::exporter_otlp_http_log
                                 opentelemetry-cpp::metrics
                                 opentelemetry-cpp::common
                                 opentelemetry-cpp::api)
   target_compile_definitions(uFilterPickerPickBroker PRIVATE WITH_OTLP_GRPC)
else()
   target_link_libraries(uFilterPickerPickBroker
                         PRIVATE opentelemetry-cpp::otlp_http_log_record_exporter
                                 opentelemetry-cpp::otlp_http_metric_exporter)
endif()
target_link_libraries(uFilterPickerPickBroker
                      PRIVATE uFilterPickerPickBroker::libuFilterPickerPickBroker
                              spdlog::spdlog_header_only
                              protobuf::libprotobuf gRPC::grpc gRPC::grpc++
                              Boost::headers Boost::program_options)


if (PROJECT_IS_TOP_LEVEL AND ${BUILD_TESTS})
   add_executable(unitTests
                  testing/database.cpp
                  testing/options.cpp
                  testing/pickStore.cpp
                  testing/service.cpp
                  uFilterPickerPickBrokerAPI/v1/stream_identifier.proto
                  uFilterPickerPickBrokerAPI/v1/phase_hint.proto
                  uFilterPickerPickBrokerAPI/v1/pick.proto
                  uFilterPickerPickBrokerAPI/v1/publish_service.proto
                  uFilterPickerPickBrokerAPI/v1/subscribe_service.proto
                  uFilterPickerPickBrokerAPI/v1/publish_response.proto
                  uFilterPickerPickBrokerAPI/v1/stream_since_request.proto
   )
   set_target_properties(unitTests PROPERTIES
                         CXX_STANDARD 23
                         CXX_STANDARD_REQUIRED YES
                         CXX_EXTENSIONS NO)
   target_link_libraries(unitTests
                         PRIVATE
                            uFilterPickerPickBroker::libuFilterPickerPickBroker
                            spdlog::spdlog_header_only
                            gRPC::grpc gRPC::grpc++
                            Threads::Threads
                            Catch2::Catch2 Catch2::Catch2WithMain)
   target_include_directories(unitTests
                              PRIVATE $<BUILD_INTERFACE:${CMAKE_CURRENT_BINARY_DIR}>)
   add_test(NAME unitTests
            COMMAND $<TARGET_FILE:unitTests>)
   protobuf_generate(TARGET unitTests
                     LANGUAGE cpp
                     )
   protobuf_generate(TARGET unitTests
                     LANGUAGE grpc
                     PLUGIN "protoc-gen-grpc=$<TARGET_FILE:gRPC::grpc_cpp_plugin>"
                     PLUGIN_OPTIONS generate_mock_code=true
                     GENERATE_EXTENSIONS .grpc.pb.h .grpc.pb.cc
                    )
endif()


##########################################################################################
#                                      Installation                                      #
##########################################################################################
include(GNUInstallDirs)
include(CMakePackageConfigHelpers)
configure_package_config_file(
    cmake/${PROJECT_NAME}Config.cmake.in "${CMAKE_CURRENT_BINARY_DIR}/${PROJECT_NAME}Config.cmake"
    INSTALL_DESTINATION "${CMAKE_INSTALL_LIBDIR}/cmake/${PROJECT_NAME}")
write_basic_package_version_file(
    "${CMAKE_CURRENT_BINARY_DIR}/${PROJECT_NAME}ConfigVersion.cmake"
    VERSION "${version}"
    COMPATIBILITY AnyNewerVersion
)

install(TARGETS libuFilterPickerPickBroker
        EXPORT ${PROJECT_NAME}Targets
        FILE_SET HEADERS
        LIBRARY DESTINATION ${CMAKE_INSTALL_LIBDIR}
        ARCHIVE DESTINATION ${CMAKE_INSTALL_LIBDIR}
        COMPONENT libraries)
install(TARGETS uFilterPickerPickBroker
        RUNTIME DESTINATION ${CMAKE_INSTALL_BINDIR}
        COMPONENT applications)
install(EXPORT ${PROJECT_NAME}Targets
        NAMESPACE ${PROJECT_NAME}::
        FILE ${PROJECT_NAME}Targets.cmake
        DESTINATION ${CMAKE_INSTALL_LIBDIR}/cmake/${PROJECT_NAME})
install(FILES
        ${CMAKE_CURRENT_BINARY_DIR}/${PROJECT_NAME}Config.cmake
        ${CMAKE_CURRENT_BINARY_DIR}/${PROJECT_NAME}ConfigVersion.cmake
        DESTINATION ${CMAKE_INSTALL_LIBDIR}/cmake/${PROJECT_NAME})

##########################################################################################
#                                        Containers                                      #
##########################################################################################
include(CPackComponent)
if (${BUILD_CONTAINER})
   message("Will configure container")
   configure_file(${CMAKE_CURRENT_SOURCE_DIR}/scripts/Dockerfile.u26.in
                  ${CMAKE_CURRENT_BINARY_DIR}/Dockerfile.u26 @ONLY)
   add_custom_target(${PROJECT_NAME}-u26
                     COMMENT "Creating container file for ${PROJECT_NAME}"
                     COMMAND ${CMAKE_CPACK_COMMAND} -G DEB
                     WORKING_DIRECTORY ${CMAKE_CURRENT_BINARY_DIR} VERBATIM)
   add_dependencies(${PROJECT_NAME}-u26 uFilterPickerPickBroker libuFilterPickerPickBroker)
   find_program(buildah_EXECUTABLE buildah)
   if (buildah_EXECUTABLE)
      add_custom_target(buildah
                        COMMENT "Preparing the container with buildah"
                        COMMAND ${buildah_EXECUTABLE} bud -t ufilterpicker:${PROJECT_VERSION} -t ufilterpicker:latest -f ./Dockerfile.u26
                        WORKING_DIRECTORY ${CMAKE_CURRENT_BINARY_DIR} VERBATIM)
      add_dependencies(buildah ${PROJECT_NAME}-u26)
   endif()
endif()

##########################################################################################
#                                     CPACK Packaging                                    #
##########################################################################################
set(CPACK_PACKAGE_NAME ${PROJECT_NAME})
set(CPACK_PACKAGE_VENDOR "UUSS")
set(CPACK_PACKAGE_CONTACT "ben.baker@utah.edu")
set(CPACK_PACKAGE_LICENSE "MIT")
set(CPACK_PACKAGE_DESCRIPTION_SUMMARY "A pick broker for the pub-sub pattern to which instances of the uFilterPicker can submit their picks in the UUSS Kubernetes environment.")
set(CPACK_PACKAGE_INSTALL_DIRECTORY ${CPACK_PACKAGE_NAME})
set(CPACK_VERBATIM_VARIABLES YES)
set(CPACK_PACKAGE_VERSION ${PROJECT_VERSION})
set(CPACK_PACKAGE_VERSION_MAJOR ${PROJECT_VERSION_MAJOR})
set(CPACK_PACKAGE_VERSION_MINOR ${PROJECT_VERSION_MINOR})
set(CPACK_PACKAGE_VERSION_PATCH ${PROJECT_VERSION_PATCH})
set(CPACK_RESOURCE_FILE_LICENSE "${CMAKE_SOURCE_DIR}/LICENSE")
if (${BUILD_SHARED_LIBS})
   cpack_add_component(libraries
                       DISPLAY_NAME "uFilterPickerPickBroker common libraries"
                       DEPENDS ${GRPC_LIBRARY} ${GRPC_GRPC++_LIBRARY})
else()
   cpack_add_component(libraries
                       DISPLAY_NAME "uFilterPickerPickBroker common libraries")
endif()
cpack_add_component(headers
                    DISPLAY_NAME "uFilterPickerPickBroker library header files"
                    DEPENDS libraries)
cpack_add_component(applications
                    DISPLAY_NAME "uFilterPickerPickBroker applications"
                    DEPENDS libraries)
set(CPACK_GENERATOR ZIP TGZ)
if (WIN32)
   list(APPEND CPACK_GENERATOR WIX)
elseif (APPLE)
   list(APPEND CPACK_GENERATOR productbuild)
elseif(CMAKE_SYSTEM_NAME STREQUAL "Linux")
   find_program(RPMBUILD_PROGRAM rpmbuild)
   if (RPMBUILD_PROGRAM)
      list(APPEND CPACK_GENERATOR RPM)
   endif()
   if (${BUILD_FOR_DEBIAN})
      find_program(DPKG_PROGRAM dpkg REQUIRED)
   else()
      find_program(DPKG_PROGRAM dpkg)
   endif()
   if (DPKG_PROGRAM)
      list(APPEND CPACK_GENERATOR DEB)
      set(CPACK_DEBIAN_PACKAGE_DEPENDS "${CPACK_DEBIAN_PACKAGE_DEPENDS}")
      set(CPACK_DEBIAN_PACKAGE_SHLIBDEPS ON)
   endif()
else()
   message("Unknown operating system")
endif()
set(CPACK_SOURCE_IGNORE_FILES
  /\\.git/
  \\.swp
  \\.orig
  /CMakeLists\\.txt\\.user
  /private/
)
include(CPack) # Put this last!
