#ifndef BASE_XLIB_XBYTEBUFFER_H_
#define BASE_XLIB_XBYTEBUFFER_H_
#include <ext/mt_allocator.h>
#include <string.h>
#include <vector>
#include <queue>
#include "xlib/xDefine.h"
#define  MAX_BUFSIZE 65536
class xByteBuffer
{
  public:
    xByteBuffer();
    ~xByteBuffer();

    UINT GetLeft() { return buffer_size_ - buffer_offset_; }
    void *GetBufOffset() { return &(byte_buffer_[buffer_offset_]); }
    void *GetBufOffset(UINT offset) { return &(byte_buffer_[offset]); }
    void *GetBufBegin() const { return &(byte_buffer_[0]); }
    void Copy(const xByteBuffer& buffer);

    UINT Put(const void *data, UINT len);
    UINT Put(UINT len);
    UINT Pop(UINT len);

    void Resize(UINT len);
    void Reset() { set_buffer_offset(0); }

    void set_buffer_offset(INT buffer_offset) { buffer_offset_ = buffer_offset; }
    UINT buffer_offset() const { return buffer_offset_; }
    void set_buffer_size(INT buffer_size) { buffer_size_ = buffer_size; }
    UINT buffer_size() const { return buffer_size_; }

  private:
    BYTE *byte_buffer_;
    UINT buffer_size_;
    UINT buffer_offset_;

    __gnu_cxx::__mt_alloc<unsigned char> __mt_alloc;
};
#endif  // BASE_XLIB_XBYTEBUFFER_H_
