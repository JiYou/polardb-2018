set(CMAKE_INCLUDE_CURRENT_DIR ON)
include_directories(${CMAKE_CURRENT_SOURCE_DIR}/../)

# Add OpenCV Static Libaries.
IF (WIN32)
    # If want to use static library, change the path to
    # ${CMAKE_CURRENT_SOURCE_DIR}/lib/win32
    set(SYS_LIB glu32 comctl32 gdi32 ole32 setupapi ws2_32 vfw32 winmm opengl32 glu32 kernel32 user32 gdi32 winspool shell32 ole32 oleaut32 uuid comdlg32 advapi32)
ELSE ()
    set(SYS_LIB pthread m rt)
ENDIF()

add_library(etest_main etest_main.cc etest.cc)
target_link_libraries(etest_main)
SET(TEST_LIB etest_main)

add_executable(test_etest etest_test.cc )
#add_dependencies(test_etest ${TEST_LIB})
target_link_libraries(test_etest ${TEST_LIB})
