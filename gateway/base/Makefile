XLIB_DIR = ./xlib
XLIB_SRCS = $(wildcard $(XLIB_DIR)/*.cpp)
XLIB_OBJECTS = $(patsubst %.cpp, %.o,$(XLIB_SRCS))
XLIB_CFLAGS = -O2 -fno-short-enums

BASE_OBJECTS = log4cxxconfigurator.o
OBJECTS = $(BASE_OBJECTS) $(XLIB_OBJECTS)
TARGET = libX.a

.PHONY : all

all: $(TARGET)
$(TARGET):$(OBJECTS)
	ar rcu $(TARGET) $(OBJECTS)
	ranlib $(TARGET)

$(BASE_OBJECTS): %.o: %.cpp 
	$(CXX) $(CXXFLAGS) -c $< -o $@ 

SRCS = $(BASE_OBJECTS:%.o=%.cpp)
DEPS = $(BASE_OBJECTS:%.o=.%.d)

include $(DEPS)
$(DEPS): .%.d: %.cpp 
	@$(CXX) -MM $(CXXFLAGS) $< > $@.$$$$;sed 's,\($*\)\.o[ :]*,\1.o $@ : ,g' < $@.$$$$ > $@; rm -f $@.$$$$

.PHONY: clean
clean:
	rm  -rf $(OBJECTS) $(TARGET)
