
typedef void* SQLHANDLE;
typedef short SQLSMALLINT;
typedef SQLSMALLINT SQLRETURN;

/*typedef SQLRETURN  ( *SQLAllocHandleType)(SQLSMALLINT HandleType,
           SQLHANDLE InputHandle, SQLHANDLE *OutputHandle)
;
*/
#include <stdio.h>
#include <Windows.h>

typedef SQLRETURN __attribute__((stdcall)) (*SQLAllocHandleType)(SQLSMALLINT HandleType,
           SQLHANDLE InputHandle, SQLHANDLE *OutputHandle);

int Call_SQLAllocHandle(void *f) {
    HMODULE odbc32 = LoadLibraryA("odbc32.dll");
    
    FARPROC alloc_handle = GetProcAddress(odbc32, "SQLAllocHandle");
    
    printf("odbc32.dll = %p, alloc_handle = %p\n", odbc32, alloc_handle);
    
    SQLAllocHandleType t = (SQLAllocHandleType)alloc_handle;
    SQLHANDLE handle = 0;
    SQLRETURN ret = t(1, 0, &handle);
    
    printf("ret = %d\n", (int)ret);
    printf("handle = %p\n", handle);
    
    return 17;
}