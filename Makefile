# Makefile for join_optimizer PostgreSQL extension

EXTENSION = join_optimizer
MODULE_big = join_optimizer

# Source files in src/ directory
OBJS = src/join_optimizer_main.o \
       src/stats.o \
       src/algorithms.o \
       src/cost.o \
       src/paths.o \
       src/utils.o \
       src/stats_collector.o

# SQL files - reference files in sql/ directory
DATA = sql/join_optimizer--1.0.sql

# Regression tests configuration
# Tests are in regression/sql/, expected output in regression/expected/
# Results go to regression/results/, regression/regression.diffs, regression/regression.out
REGRESS = join_optimizer_regtest
REGRESS_OPTS = --inputdir=regression --outputdir=regression

PGFILEDESC = "join_optimizer - Statistics-based join order optimizer"

# Include paths
PG_CPPFLAGS = -I$(srcdir)/include -I$(libpq_srcdir)
SHLIB_LINK_INTERNAL = $(libpq)

# PostgreSQL build system - default to PGXS for out-of-tree builds
PG_CONFIG ?= pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)

# Additional targets
.PHONY: install-scripts test clean-all regression

# Test target  
test: install
	psql -d postgres -c "CREATE EXTENSION IF NOT EXISTS join_optimizer;"
	psql -d postgres -c "SELECT join_optimizer.enable();"
	psql -d postgres -c "SELECT * FROM join_optimizer.stats_summary LIMIT 5;"

# Run regression tests
regression: install
	psql -d postgres -f regression/test.sql

# Clean all generated files
clean-all: clean
	rm -f src/*.o *.so *.bc
	rm -rf regression/results regression/regression.diffs regression/regression.out

# Format source code (requires clang-format)
format:
	clang-format -i src/*.c include/*.h

# Lint: Check for common issues (use 'lint' to avoid conflict with PGXS 'check')
lint:
	@echo "Checking for common issues..."
	@grep -rn "palloc\\|pfree" src/ | head -20
	@echo "Done."

# Install SQL files to proper location
install-data:
	$(INSTALL_DATA) sql/join_optimizer--1.0.sql '$(DESTDIR)$(datadir)/extension/'
