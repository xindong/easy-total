#include "xlib/xByteBuffer.h"

xByteBuffer::xByteBuffer()
{
  buffer_offset_ = 0;
  buffer_size_ = 0;
  byte_buffer_ = NULL;

  Resize(MAX_BUFSIZE * 2);
}

xByteBuffer::~xByteBuffer(void)
{
  if (byte_buffer_)
  {
    __mt_alloc.deallocate((unsigned char *)byte_buffer_, buffer_size());
    byte_buffer_ = NULL;
    buffer_offset_ = 0;
    buffer_size_ = 0;
  }
}

void xByteBuffer::Resize(UINT len)
{
  if (len <= buffer_offset()) return;

  unsigned char *buf = __mt_alloc.allocate(len);

  if (byte_buffer_)
  {
    bcopy(&byte_buffer_[0], &buf[0], buffer_offset());
    __mt_alloc.deallocate((unsigned char *)byte_buffer_, buffer_size());
  }

  byte_buffer_ = buf;
  set_buffer_size(len);
}

UINT xByteBuffer::Put(const void *data, UINT len)
{
  //if (len > MAX_BUFSIZE)
  //{
  //  return 0;
  //}

  while (GetLeft() < len)
  {
    Resize(buffer_size() + len + MAX_BUFSIZE);
  }
  memcpy(GetBufOffset(), data, len);
  buffer_offset_ += len;
  return len;
}

UINT xByteBuffer::Put(UINT len)
{
  if ((buffer_offset() + len) > buffer_size())
  {
    return 0;
  }

  buffer_offset_ += len;

  if (GetLeft() < MAX_BUFSIZE)
  {
    Resize(buffer_size() + MAX_BUFSIZE);
  }
  return len;
}

UINT xByteBuffer::Pop(UINT len)
{
  if (len < buffer_offset())
  {
    bcopy(&byte_buffer_[len], &byte_buffer_[0], buffer_offset()-len);
    buffer_offset_ -= len;
  }
  else if (len > buffer_offset())
  {
    buffer_offset_ = 0;
  }
  else 
  {
    buffer_offset_ = 0;
  }
  return len;
}

void xByteBuffer::Copy(const xByteBuffer& buffer) 
{
  if (buffer_size() < buffer.buffer_size()) 
  {
    Resize(buffer.buffer_size());
  }
  bcopy(buffer.GetBufBegin(), GetBufBegin(), buffer.buffer_offset());
  buffer_offset_ += buffer.buffer_offset();
}
