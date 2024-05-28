#include <R.h>
#include <Rinternals.h>
#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <fcntl.h>

#define HCAP 4096
#define MAX_DISTINCT_GROUPS 512
#define MAX_GROUPBY_KEY_LENGTH 100
#define NTHREADS 16

typedef struct {
  char label[MAX_GROUPBY_KEY_LENGTH];
  unsigned int count;
  float sum;
  float min;
  float max;
} Group;

typedef struct {
  Group groups[MAX_DISTINCT_GROUPS];
  int n;
} Result;

typedef struct {
  size_t start;
  size_t end;
  const char *data;
  Result *result;
} ThreadData;

unsigned int hash(const char *data) {
  unsigned int h = 0;
  while (*data) {
    h = (h * 31) + *data++;
  }
  return h & (HCAP - 1);
}

float parse_float(const char *str) {
  float result = 0.0;
  int sign = 1;
  if (*str == '-') {
    sign = -1;
    str++;
  }
  while (*str >= '0' && *str <= '9') {
    result = result * 10.0 + (*str - '0');
    str++;
  }
  if (*str == '.') {
    str++;
    float factor = 0.1;
    while (*str >= '0' && *str <= '9') {
      result += (*str - '0') * factor;
      factor *= 0.1;
      str++;
    }
  }
  return sign * result;
}

void *process_chunk(void *arg) {
  ThreadData *td = (ThreadData *)arg;
  td->result = (Result *)malloc(sizeof(Result));
  Result *result = td->result;
  memset(result, 0, sizeof(Result));  // Initialize result

  const char *s = td->data + td->start;
  const char *end = td->data + td->end;

  // Make sure we start and end on whole lines
  if (td->start != 0) {
    while (*s != '\n' && s < end) s++;
    s++;
  }
  if (s < end && *(end - 1) != '\n') {
    while (*end != '\n' && end > td->data) end--;
  }

  char line[256];
  while (s < end) {
    const char *line_end = strchr(s, '\n');
    if (!line_end) line_end = end;
    int len = line_end - s;
    if (len >= sizeof(line)) len = sizeof(line) - 1;
    strncpy(line, s, len);
    line[len] = '\0';

    char *sep = strchr(line, ';');
    if (sep) {
      *sep = '\0';
      float temp = parse_float(sep + 1);
      unsigned int idx = hash(line) % MAX_DISTINCT_GROUPS;
      Group *group = &result->groups[idx];

      if (group->count == 0) {
        strncpy(group->label, line, MAX_GROUPBY_KEY_LENGTH);
        group->label[MAX_GROUPBY_KEY_LENGTH - 1] = '\0';
        group->min = group->max = temp;
        group->sum = temp;
        group->count = 1;
        result->n++;
      } else {
        if (temp < group->min) group->min = temp;
        if (temp > group->max) group->max = temp;
        group->sum += temp;
        group->count++;
      }
    }
    s = line_end + 1;
  }
  return NULL;
}

SEXP calculate_stats(SEXP Rfilepath) {
  const char *filepath = CHAR(STRING_ELT(Rfilepath, 0));
  int fd = open(filepath, O_RDONLY);
  if (fd == -1) {
    Rf_error("Cannot open file: %s", filepath);
    return R_NilValue;
  }

  struct stat sb;
  if (fstat(fd, &sb) == -1) {
    close(fd);
    Rf_error("Cannot get file size: %s", filepath);
    return R_NilValue;
  }

  size_t size = (size_t)sb.st_size;
  const char *data = (const char *)mmap(NULL, size, PROT_READ, MAP_SHARED, fd, 0);
  close(fd);
  if (data == MAP_FAILED) {
    Rf_error("Cannot map file: %s", filepath);
    return R_NilValue;
  }

  pthread_t threads[NTHREADS];
  ThreadData tdata[NTHREADS];
  size_t chunk_size = size / NTHREADS;
  for (int i = 0; i < NTHREADS; i++) {
    tdata[i].start = i * chunk_size;
    tdata[i].end = (i + 1) * chunk_size;
    tdata[i].data = data;
    if (i == NTHREADS - 1) tdata[i].end = size;
    pthread_create(&threads[i], NULL, process_chunk, &tdata[i]);
  }

  Result final_result;
  memset(&final_result, 0, sizeof(final_result));

  for (int i = 0; i < NTHREADS; i++) {
    pthread_join(threads[i], NULL);
    Result *thread_result = tdata[i].result;

    // Merge results from thread into final_result
    for (int j = 0; j < MAX_DISTINCT_GROUPS; j++) {
      Group *group = &thread_result->groups[j];
      if (group->count > 0) {
        unsigned int idx = hash(group->label) % MAX_DISTINCT_GROUPS;
        Group *final_group = &final_result.groups[idx];

        if (final_group->count == 0) {
          strncpy(final_group->label, group->label, MAX_GROUPBY_KEY_LENGTH);
          final_group->min = group->min;
          final_group->max = group->max;
          final_group->sum = group->sum;
          final_group->count = group->count;
          final_result.n++;
        } else {
          final_group->min = (group->min < final_group->min) ? group->min : final_group->min;
          final_group->max = (group->max > final_group->max) ? group->max : final_group->max;
          final_group->sum += group->sum;
          final_group->count += group->count;
        }
      }
    }
    free(thread_result);
  }

  munmap((void *)data, size);

  if (final_result.n == 0) {
    return R_NilValue;
  }

  SEXP res = PROTECT(allocVector(VECSXP, 4));
  SEXP names = PROTECT(allocVector(STRSXP, final_result.n));
  SEXP mins = PROTECT(allocVector(REALSXP, final_result.n));
  SEXP means = PROTECT(allocVector(REALSXP, final_result.n));
  SEXP maxs = PROTECT(allocVector(REALSXP, final_result.n));

  int idx = 0;
  for (int i = 0; i < MAX_DISTINCT_GROUPS; i++) {
    if (final_result.groups[i].count > 0) {
      SET_STRING_ELT(names, idx, mkChar(final_result.groups[i].label));
      REAL(mins)[idx] = final_result.groups[i].min;
      REAL(means)[idx] = final_result.groups[i].sum / (double)final_result.groups[i].count;
      REAL(maxs)[idx] = final_result.groups[i].max;
      idx++;
    }
  }

  SET_VECTOR_ELT(res, 0, names);
  SET_VECTOR_ELT(res, 1, mins);
  SET_VECTOR_ELT(res, 2, means);
  SET_VECTOR_ELT(res, 3, maxs);

  UNPROTECT(5);
  return res;
}

static const R_CallMethodDef CallEntries[] = {
  {"calculate_stats", (DL_FUNC) &calculate_stats, 1},
  {NULL, NULL, 0}
};

void R_init_tempstats(DllInfo *dll) {
  R_registerRoutines(dll, NULL, CallEntries, NULL, NULL);
  R_useDynamicSymbols(dll, FALSE);
}
