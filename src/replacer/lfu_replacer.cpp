#include "lfu_replacer.h"

lfuReplacer::lfuReplacer(size_t num_pages) {
    
}

lfuReplacer::~lfuReplacer() = default;


bool lfuReplacer::victim(frame_id_t *frame_id){}

void lfuReplacer::pin(frame_id_t frame_id){}

void lfuReplacer::unpin(frame_id_t frame_id){}

size_t lfuReplacer::Size(){}
