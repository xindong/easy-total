
MAKE = make
CC = distcc
CLIBS =  -Wl,-E -ldl -lreadline -lhistory -lncurses -lm 
CXX = distcc g++
LIBS = ../base/libX.a 
export CLIBS LIBS

DEBUG_CXXFLAGS = -L../base -I../base -I../base/xlib/ -I/usr/include/libxml2 -D_DEBUG $(DEBUG_FLAG) -g3 -Wall -Wno-unknown-pragmas --std=c++0x -Werror
DEBUG_CXXLFLAGS = -L../base -I../base -I../base/xlib/ -L/usr/lib64/ -lX -lxml2 -lpthread -llog4cxx -ldl

DEFS = -D_YOU_NAME
DEBUG_CXXFLAGS += $(DEFS)

DIST_CXXFLAGS = -L../base -I../base -I../base/xlib/ -I/usr/include/libxml2 -Wall -Wno-unknown-pragmas --std=c++0x -Werror -g3 -O2 -fno-strict-aliasing -fno-short-enums -fno-schedule-insns #优化打开
DIST_CXXLFLAGS = -L../base -I../base -I../base/xlib/ -L/usr/lib64/ -lX -lxml2 -lpthread -llog4cxx -ldl

SUBDIRS = base LogGate 

.PHONY: all debug dist dist_build clean tag svn $(SUBDIRS)

all: debug


debug: $(SUBDIRS)
	@echo "--------- 警告: 当前为调试版本 ---------"
LogGate: base
	@CXXLFLAGS='$(DEBUG_CXXLFLAGS)' CXXFLAGS='$(DEBUG_CXXFLAGS)' $(MAKE) -C LogGate || exit $$?

base:
	@CXXLFLAGS='$(DEBUG_CXXLFLAGS)' CXXFLAGS='$(DEBUG_CXXFLAGS)' $(MAKE) -C base || exit $$?

dist: 
	@for dir in $(SUBDIRS);\
		do\
			$(MAKE) clean -C $$dir;\
			if [ -f $$dir/$$dir.symbol ]; then\
				rm $$dir/$$dir.symbol;\
			fi\
		done
	@for dir in $(SUBDIRS); \
		do \
		CXXLFLAGS='$(DIST_CXXLFLAGS)' CXXFLAGS='$(DIST_CXXFLAGS)' $(MAKE) -C $$dir || exit 1; \
		done; \
	echo "--------- 注意: 当前为发布版本 ---------"; 
	@for dir in $(SUBDIRS);\
		do \
			if [ -f $$dir/$$dir ]; then\
				mv $$dir/$$dir $$dir/$$dir.symbol;\
				strip -s $$dir/$$dir.symbol -o $$dir/$$dir;\
			fi\
		done
	@echo "--------- 转移符号表文件 ---------"; 

clean:
	@for dir in $(SUBDIRS);\
		do\
			$(MAKE) clean -C $$dir;\
			if [ -f $$dir/$$dir.symbol ]; then\
				rm $$dir/$$dir.symbol;\
			fi\
		done

clear:
	@for dir in $(SUBDIRS);\
		do\
			rm $$dir/*.o;\
		done

tag:
	@ctags -R --c++-kinds=+p --fields=+iaS --extra=+q

svn:
	svn propset svn:ignore -F .svnignore .
	@for dir in $(SUBDIRS); do svn propset svn:ignore -F $$dir/.svnignore $$dir; done

