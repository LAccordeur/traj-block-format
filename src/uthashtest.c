//
// Created by yangguo on 11/21/22.
//

#include "uthash/uthash.h"
#include <stdio.h>

struct my_struct {
    int id;                    /* key */
    int value;
    UT_hash_handle hh;         /* makes this structure hashable */
};

struct my_struct *users = NULL;    /* important! initialize to NULL */

void add_user(int user_id, int value) {
    struct my_struct *s;

    HASH_FIND_INT(users, &user_id, s);
    if (s == NULL) {
        //printf("size: %d\n", sizeof *s);
        s = malloc(sizeof *s);
        s->id = user_id;
        s->value = value;
        HASH_ADD_INT(users, id, s);  /* id: name of key field */
    }
}

struct my_struct *find_user(int user_id) {
    struct my_struct *s;

    HASH_FIND_INT(users, &user_id, s);  /* s: output pointer */
    return s;
}

void delete_user(struct my_struct *user) {
    HASH_DEL(users, user);  /* user: pointer to deletee */
    free(user);             /* optional; it's up to you! */
}

void delete_all() {
    struct my_struct *current_user, *tmp;

    HASH_ITER(hh, users, current_user, tmp) {
        HASH_DEL(users, current_user);  /* delete; users advances to next */
        free(current_user);             /* optional- if you want to free  */
    }
}

void print_users() {
    struct my_struct *s;

    for (s = users; s != NULL; s = s->hh.next) {
        printf("user id %d: name %d\n", s->id, s->value);
    }
}


int main() {
    add_user(1, 3);
    int num_users;
    num_users = HASH_COUNT(users);
    printf("there are %u users\n", num_users);
}