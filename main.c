#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <omp.h>

/*
 * The majority of the sequential code in this project is replicated across..-
 * -.. all versions of this algorithm - MPI, Pthreads and OpenMP.
 */


int getSequenceData(FILE* file, int* sequenceCount, int* maxSequenceLength) {
    fscanf(file, "%d", sequenceCount);
    fscanf(file, "%d", maxSequenceLength);

    if (!*sequenceCount || *sequenceCount < 0 ||
        !*maxSequenceLength || *maxSequenceLength < 0) {
        printf("Invalid input.\n");
        return 0;
    }
    return 1;
}

int malloc_pattern_arrays(char**** patterns, size_t** patternLengths,
                          int elementCount) {
    *patterns = (char***) malloc(elementCount * sizeof(char**));
    if (*patterns) {
        *patternLengths = (size_t *) malloc(elementCount * sizeof(size_t));
        if (!*patternLengths) {
            free(*patterns);
            return 0;
        }
        return 1;
    }
    return 0;
}

void free_inner_2DArray(void** array, int length) {
    for (int i = 0; i < (int) length; i++) {
        free(array[i]);
    }
}

void free_full_2DArray(void** array, int length) {
    free_inner_2DArray(array, length);
    free(array);
}


int malloc_pattern_array(char*** array1, size_t elementCount, int repeat) {
    // The function parameters are specific for less complexity as this is the only purpose they are used for.
    for (int i = 0; i < repeat; i++) {
        array1[i] = (char**) malloc(elementCount * sizeof(char *));
        if (!array1[i]) {
            free_full_2DArray((void **) array1, i);
            return 0;
        }
    }
    return 1;
}

void free_pattern_arrays(char*** patterns, size_t* patternLengths,
                         int repeats, int size, int* patternCount) {
    for (int i = 0; i < repeats; i++) {
        free_inner_2DArray((void**) patterns[i], patternCount[i]);
    }
    free_full_2DArray((void**) patterns, size);
    free(patternLengths);
}

FILE* getFile(char* fileName) {
    FILE* file = fopen(fileName, "r");
    return file;
}

int distinguishPatterns(FILE* file, char*** patterns, size_t* patternLengths,
                        int* patternCount, int sequenceCount,
                        int estimatedPatternCount) {
    char patternBuffer[255]; // Max length of a pattern
    int activeBracket = 0;
    int k;
    int currentVariations;
    size_t currentLength;
    char currentLetter;
    for (int i = 0; i < sequenceCount; i++){
        fscanf(file, "%s", patternBuffer);
        currentLength = strlen(patternBuffer);
        char withoutVariation[currentLength]; // Pattern before variable input
        currentVariations = 0;
        k = 0;
        for (int j = 0; j < (int) currentLength; j++) {
            currentLetter = patternBuffer[j];
            if (currentLetter == '[') {
                // Embedded brackets or more than one variation in the same pattern
                if (activeBracket || currentVariations) {
                    printf("Pattern %d is invalid.\nEmbedded brackets are not"
                           " allowed and only one variation is permitted"
                           " within each pattern.\n", i);
                    free_pattern_arrays(patterns, patternLengths, i,
                                        sequenceCount, patternCount);
                    return 0;
                }
                withoutVariation[k] = '\0';
                activeBracket++;
                continue;
            }
            if (currentLetter == ']') {
                // No corresponding open bracket, or empty bracket
                if (!activeBracket || !currentVariations) {
                    printf("Pattern %d is invalid.\nClosing brackets "
                           "must have corresponding opening brackets and "
                           "brackets may not be empty.\n", i);
                    free_pattern_arrays(patterns, patternLengths, i,
                                        sequenceCount, patternCount);
                    return 0;
                }
                // Variation within current pattern already exists - limited to 1.
                activeBracket--;
                k = 0;
                continue;
            }
            if (activeBracket) {
                if (currentVariations == estimatedPatternCount) {
                    patterns[i] = (char**) realloc(
                            patterns[i],currentVariations*2 * sizeof(char*));

                    if (!patterns[i] || patterns[i][currentVariations]) {
                        free_pattern_arrays(patterns, patternLengths, i,
                                            sequenceCount, patternCount);
                        return 0;
                    }
                }
                // currentLength is either the whole length or includes 2 brackets.
                patterns[i][currentVariations] = (char*)
                        malloc((currentLength-2+1) * sizeof(char));
                if (!patterns[i][currentVariations]) {
                    free_pattern_arrays(patterns, patternLengths, i,
                                        sequenceCount, patternCount);
                    return 0;
                }
                // Store one variation of the pattern
                strncpy(patterns[i][currentVariations], withoutVariation, k);
                patterns[i][currentVariations][k] = '\0';
                strncat(patterns[i][currentVariations], &currentLetter, 1);
                patternCount[i]++;
                currentVariations++;
            }
            else {
                withoutVariation[k] = currentLetter;
                k++;
            }
        }
        withoutVariation[k] = '\0';
        if (!currentVariations) {
            patterns[i] = (char**) realloc(patterns[i], 1 * sizeof(char*));
            if (patterns[i]) {
                patterns[i][0] = (char*) malloc((k+1) * sizeof(char));
            }
            if (!patterns[i] || !patterns[i][0]) {
                free_pattern_arrays(patterns, patternLengths, i,
                                    sequenceCount, patternCount);
                return 0;
            }
            strncpy(patterns[i][0], withoutVariation, k+1);
            patternLengths[i] = k;
            patternCount[i] = 1;
            continue;
        }
        currentLength = 0;
        for (int j = 0; j < currentVariations; j++) {
            strncat(patterns[i][j], withoutVariation, k+1);
            if (!currentLength) { // All patterns variations are of the same length
                currentLength = strlen(patterns[i][j]);
            }
            patternLengths[i] = currentLength;
            patterns[i][j] = (char*) realloc(patterns[i][j],
                                             (currentLength + 1) * sizeof(char));
            if (!patterns[i][j]) {
                free_pattern_arrays(patterns, patternLengths, i,
                                    sequenceCount, patternCount);
                return 0;
            }
        }
        patternCount[i] = currentVariations;
    }
    return 1;
}

int parseSequences(FILE* file, char** sequences, int sequenceCount, int maxSequenceLength) {
    char seqBuffer[maxSequenceLength+1];
    size_t currentLength;
    for (int i = 0; i < sequenceCount; i++) {
        fscanf(file, "%s", seqBuffer);
        currentLength = strlen(seqBuffer);
        sequences[i] = (char*) malloc((currentLength+1) * sizeof(char));
        if (!sequences[i]) {
            free_full_2DArray((void**) sequences, i); // Free all up to i
            return 0;
        }
        seqBuffer[currentLength] = '\0';
        strncpy(sequences[i], seqBuffer, currentLength+1);
    }
    return 1;
}

void findCommonStart(char* commonStart, char** patterns, size_t patternLength,
                     int index, const int* patternCount) {
    for (int j = 0; j < (int) patternLength; j++) {
        for (int k = 1; k < patternCount[index]; k++) {
            if (patterns[k][j] != patterns[k - 1][j]) {
                commonStart[j] = '\0';
                j = patternLength; // End outer loop - factor whole loop into function later and return instead
                break;
            }
            commonStart[j] = patterns[0][j]; // index 0 is allowed because all instances of the first letter are the same per the above loop
        }
    }
}

int patternMatchCommonStart(int patternCount, size_t patternLength,
                            const char* matchedString, char** patterns) {
    for (int j = 0; j < patternCount; j++) {
        for (int k = 0; k < (int) patternLength; k++) {
            if (matchedString[k] != patterns[j][k]) {
                break;
            }
            if (k == (int) (patternLength-1)) return 1; // Found
        }
    }
    return 0;
}

int findMatches(char** currentPatterns, char* sequence, char* commonStart,
                int** foundMatches, int* currMatchCounter, size_t* currentSize,
                int patternCount, int currIndex, size_t patternLength, int offset) {
    char* temp = sequence;
    int found, index;
    while (temp[0] != '\0') { // End of sequence
        char* commonStartMatch = strstr(temp, commonStart); // Shortcut - initially only look for common start
        if (!commonStartMatch) break; // Not found
        found = patternMatchCommonStart(patternCount, patternLength, commonStartMatch,
                                        currentPatterns);
        if (found) {
            index = (int) (commonStartMatch - sequence) + offset;
            #pragma omp critical
            {
                foundMatches[currIndex][*currMatchCounter] = index;
                (*currMatchCounter)++;
            }
            temp = &(temp[commonStartMatch - temp + 1]);
        }
        else temp = &temp[1]; // Skip one character to remove the already-found common start
        if (*currMatchCounter >= (int) *currentSize) { // Enlarge array
            #pragma omp critical
            {
                (*currentSize) *= 2;
                foundMatches[currIndex] = (int *)
                        realloc(foundMatches[currIndex], (*currentSize) * sizeof(int));
            }
            if (!foundMatches[currIndex]) {
                return 0;
            }
        }
    }
    return 1;
}

int parallel_manageMatches(char** currentPatterns, char* sequence, char* commonStart,
                           int** foundMatches, int* currMatchCounter, size_t* currentSize,
                           int patternCount, int currIndex, int extraThreads,
                           size_t patternLength) {
    omp_set_dynamic(0);

    size_t sequenceLength = strlen(sequence);
    if (extraThreads > (int) (sequenceLength / patternLength)) return 0;

    int remainder;
    remainder = (int) sequenceLength % extraThreads;

    int errorOccurred = 0;

    #pragma omp parallel \
    default(none) \
    shared(sequenceLength, extraThreads, remainder, patternLength, errorOccurred) \
    shared(sequence, currentPatterns, commonStart, foundMatches, currMatchCounter) \
    shared(patternCount, currIndex, currentSize) \
    num_threads(extraThreads)
    {
        int portion, start;
        portion = (int) sequenceLength / extraThreads;

        int rank = omp_get_thread_num();
        if (rank < remainder) {
            portion = portion + 1;
            start = rank * portion;
        }
        else {
            portion = portion;
            start = portion*(rank - remainder) + remainder*(portion + 1);
        }
        // Start earlier in case split is at a matching case
        if (start) { // Not the first chunk
            start -= (int) patternLength - 1;
            portion += (int) patternLength - 1;
        }
        char *localSequence = (char *) malloc(portion + 1 * sizeof(char));
        if (!localSequence) errorOccurred = 1;

        memcpy(localSequence, &(sequence[start]), portion * sizeof(char));
        localSequence[portion] = '\0';
        if (!findMatches(currentPatterns, localSequence, commonStart,
                         foundMatches, currMatchCounter, currentSize,
                         patternCount, currIndex, patternLength, start)) {
            free(localSequence);
            errorOccurred = 1;
        }
        free(localSequence);
    }
    if (errorOccurred) {
        printf("An error has occurred.\n");
        return 0;
    }
    return 1;
}

int manageMatches(char*** patterns, char** sequences, int** foundMatches,
                  const size_t* patternLengths, const int* patternCount,
                  int* matchCounter, int threadCount,
                  int threadsPerThread, int sequenceCount) {

    if (threadsPerThread < 1) threadsPerThread = 1;

    omp_set_dynamic(0);
    omp_set_nested(1);
    size_t currentSizes[sequenceCount];

    int errorOccurred = 0;
    #pragma omp parallel for default(none) \
    shared(currentSizes, sequenceCount, errorOccurred, foundMatches) \
    shared(patterns, matchCounter, patternLengths, patternCount) \
    shared(sequences, threadsPerThread) \
    num_threads(threadCount)
    for (int i = 0; i < sequenceCount; i++) {
        char** currentPatterns;
        size_t patternLength, currentSize;
        currentSizes[i] = 3; // Estimate 3 finds per sequence.
        currentSize = currentSizes[i];
        foundMatches[i] = (int *) malloc(currentSize * sizeof(int));
        if (!foundMatches[i]) {
            errorOccurred = 1;
        }
        else {
            matchCounter[i] = 0;
            patternLength = patternLengths[i];
            currentPatterns = patterns[i];
            char commonStart[patternLength];
            if (patternCount[i] == 1) strcpy(commonStart, currentPatterns[0]);
            else {
                findCommonStart(commonStart, currentPatterns, patternLength,
                                i, patternCount);
            }
            if (!parallel_manageMatches(currentPatterns, sequences[i], commonStart, foundMatches,
                                        &matchCounter[i], &currentSize, patternCount[i],
                                        i, threadsPerThread, patternLength)) {
                errorOccurred = 1;
            }
        }
    }
    for (int i  = 0; i < sequenceCount; i++) {
        foundMatches[i] = realloc(foundMatches[i], matchCounter[i] * sizeof(int));
        if (!foundMatches[i] && matchCounter[i]) {
            errorOccurred = 1;
        }
    }
    return errorOccurred == 0;
}

int sortFunction (const void* itemA, const void* itemB) {
    return *(int*) itemA - *(int*) itemB;
}

int DNA_Search(FILE* file, int threadCount, int threadsPerThread) {
    if (threadCount < 1) {
        printf("Invalid input.\n");
        return 0;
    }
    int sequenceCount = 0;
    int maxSequenceLength = 0;

    if (!getSequenceData(file, &sequenceCount, &maxSequenceLength)) {
        return 0;
    }

    // Max 1 thread per sequence
    if (threadCount > sequenceCount) threadCount = sequenceCount;

    char*** patterns;
    size_t* patternLengths;
    if (!malloc_pattern_arrays(&patterns, &patternLengths, sequenceCount)) {
        return 0;
    }

    // Rough estimation that all patterns include 3 variations.
    int estimatedPatternCount = 3;
    if (!malloc_pattern_array(patterns,estimatedPatternCount, sequenceCount)) {
        return 0;
    }

    int patternCount[sequenceCount]; // Number of patterns for each sequence
    // Separate out pattern variations
    if (!distinguishPatterns(file, patterns, patternLengths,
                             patternCount, sequenceCount, estimatedPatternCount)) {
        return 0;
    }

    char** sequences = (char**) malloc(sequenceCount * sizeof(char*));
    if (!sequences) {
        free_pattern_arrays(patterns, patternLengths, sequenceCount,
                            sequenceCount, patternCount);
        return 0;
    }

    // Fill "sequences" array with sequences found in the file.
    if (!parseSequences(file, sequences, sequenceCount, maxSequenceLength)) {
        free_pattern_arrays(patterns, patternLengths, sequenceCount,
                            sequenceCount, patternCount);
        return 0;
    }

    int** foundMatches = (int**) malloc(sequenceCount * sizeof(int*));
    if (!foundMatches) {
        free_pattern_arrays(patterns, patternLengths, sequenceCount,
                            sequenceCount, patternCount);
        free_full_2DArray((void**) sequences, sequenceCount);
        return 0;
    }

    int matchCounter[sequenceCount];

    int result;

    result = manageMatches(patterns, sequences, foundMatches, patternLengths,
                           patternCount, matchCounter, threadCount,
                           threadsPerThread, sequenceCount);
    if (!result) {
        printf("An error has occurred.\n");
        return 0;
    }

    /* Sort found matches */
    for (int i = 0; i < sequenceCount; i++) {
        qsort(foundMatches[i], matchCounter[i], sizeof(int), sortFunction);
    }

    /* Print results */
//    for (int i = 0; i < sequenceCount; i++) {
//        printf("Occurrences in sequence %d: %d\n", i+1, matchCounter[i]);
//        for (int j = 0; j < matchCounter[i]; j++) {
//            printf("Occurrence %d at index: %d\n", j+1, foundMatches[i][j]);
//        }
//    }

    free_pattern_arrays(patterns, patternLengths, sequenceCount,
                        sequenceCount, patternCount);
    free_full_2DArray((void**) sequences, sequenceCount);
    free_full_2DArray((void**) foundMatches, sequenceCount);
    return 1;
}

int main() {
    for (int k = 1; k < 9; k++) {
        if (k == 3) k = 4;
        if (k == 5) k = 6;
        if (k == 7) k = 8;
        FILE *file = getFile("sequences3.txt");
        if (!file) {
            printf("File not found.\n");
            return -1;
        }
        double start, end;
        int result;
        start = omp_get_wtime();
        result = DNA_Search(file, k, 1);
        end = omp_get_wtime();
        if (!result) return -1;
        printf("Time taken: %g seconds.\n", end - start);
        FILE *f = fopen("times.txt", "a");
        fprintf(f, "%g,", end - start);
        fclose(f);
        fclose(file);
    }
    return 0;
}
