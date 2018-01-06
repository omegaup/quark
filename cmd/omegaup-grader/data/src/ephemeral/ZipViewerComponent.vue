<template>
  <div class="root d-flex flex-row h-100 bg-dark text-white">
    <div class="filenames">
      <div class="list-group">
        <button type="button" class="list-group-item list-group-item-action disabled" v-if="!zip">
          <em>Empty</em>
        </button>
        <button type="button" class="list-group-item list-group-item-action"
           v-bind:title="name" v-bind:class="{ active: active == name }"
           v-for="(item, name) in zip.files" v-else v-on:click="select(item)"
        >
          {{ name }}
        </button>
      </div>
    </div>
    <textarea class="editor" readonly>{{ contents }}</textarea>
  </div>
</template>

<script>
import * as Util from './util';

export default {
  data: function() {
    return {
      zip: null,
      active: null,
      contents: '',
    };
  },
  methods: {
    select: function(item) {
      this.active = item.name;
      item.async('string').then(value => {
        this.contents = value;
      });
    },
  },
};
</script>

<style scoped>
div.filenames {
  overflow-y: auto;
}
button.list-group-item {
  width: 10em;
  white-space: nowrap;
  overflow: hidden;
  text-overflow: ellipsis;
}
textarea {
  flex: 1;
}
</style>
