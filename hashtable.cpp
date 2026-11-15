#include <stdint.h>
#include <assert.h>
#include <malloc.h>
#include "hashtable.h"




const size_t k_max_load_factor = 8;
const size_t k_rehashing_work = 128;

static void h_init(HTab *htab,size_t n){
    assert(n>0&&((n-1)&n)==0); //n must be a power of 2
    htab->tab=(HNode**)calloc(n,sizeof(HNode *));
    htab->mask=n-1;
    htab->size=0;
}

static void h_insert(HTab *htab,HNode *node){
    size_t pos=node->hcode & htab->mask;
    HNode *next=htab->tab[pos];
    node->next=next;
    htab->tab[pos]=node;
    htab->size++;
}


static HNode **h_lookup(HTab *htab,HNode *key,bool (*eq)(HNode *,HNode *)){
    if(!htab->tab){
        return NULL;
    }
    size_t pos=key->hcode & htab->mask;
    HNode **from =&htab->tab[pos];
    for(HNode *cur;(cur=*from)!=NULL;from=&cur->next){
        if(cur->hcode==key->hcode && eq(cur,key)){
            return from; // return the pointer to the pointer of the found node for easy removal
        }
    }
    return NULL;
}

static HNode *h_detach(HTab *htab, HNode **from) {
    HNode *node = *from;    
    *from = node->next;     
    htab->size--;
    return node;
}

static void hm_trigger_rehashing(HMap *hmap) {
    hmap->older = hmap->newer;  
    h_init(&hmap->newer, (hmap->newer.mask + 1) * 2);
    hmap->migrate_pos = 0;
}


static void hm_help_rehashing(HMap *hmap) {
    size_t nwork = 0;
    while (nwork < k_rehashing_work && hmap->older.size > 0) {
        // find a non-empty slot
        HNode **from = &hmap->older.tab[hmap->migrate_pos];
        if (!*from) {
            hmap->migrate_pos++;
            continue;   // empty slot
        }
        // move the first list item to the newer table
        h_insert(&hmap->newer, h_detach(&hmap->older, from));
        nwork++;
    }
    // discard the old table if done
    if (hmap->older.size == 0 && hmap->older.tab) {
        free(hmap->older.tab);
        hmap->older = HTab{};
    }
}

HNode *hm_lookup(HMap *hmap,HNode *key,bool (*eq)(HNode*,HNode*)){
    hm_help_rehashing(hmap);
    HNode **from=h_lookup(&hmap->newer,key,eq);
    if (!from) {
        from = h_lookup(&hmap->older, key, eq);
    }
    return from ? *from : NULL;
}


HNode *hm_delete(HMap *hmap, HNode *key, bool (*eq)(HNode *, HNode *)) {
    hm_help_rehashing(hmap);
    if (HNode **from = h_lookup(&hmap->newer, key, eq)) {
        return h_detach(&hmap->newer, from);
    }
    if (HNode **from = h_lookup(&hmap->older, key, eq)) {
        return h_detach(&hmap->older, from);
    }
    return NULL;
}

void hm_insert(HMap *hmap, HNode *node) {
    if (!hmap->newer.tab) {
        h_init(&hmap->newer, 4);    // initialized it if empty
    }
    h_insert(&hmap->newer, node);   // always insert to the newer table
    if (!hmap->older.tab) {         // check whether we need to rehash
        size_t shreshold = (hmap->newer.mask + 1) * k_max_load_factor;
        if (hmap->newer.size >= shreshold) {
            hm_trigger_rehashing(hmap);
        }
    }
    hm_help_rehashing(hmap);        // migrate some keys
}

void hm_foreach(HMap *hmap,bool (*cb_keys)(HNode *,void *),void *out){
    size_t i=0;
    size_t nsize=hmap->newer.size;
    size_t osize=hmap->older.size;
    while(nsize>0){
        HNode *node=hmap->newer.tab[i];
        if(!node){
            i++;
            continue;
        }
        while(node!=NULL){
            cb_keys(node,out);
            node=node->next;
            nsize--;
        }
        i++;
    }
    i=0;
    while(osize>0){
        HNode *node=hmap->older.tab[i];
        if(!node){
            i++;
            continue;
        }
        while(node!=NULL){
            cb_keys(node,out);
            node=node->next;
            osize--;
        }
        i++;
    }

}

size_t hm_size(HMap *hmap) {
    return hmap->newer.size + hmap->older.size;
}