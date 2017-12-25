<template>
  <div class="root d-flex flex-column h-100 bg-dark text-white">
    <div class="summary">{{ summary }}</div>
    <div class="list-group filenames">
      <button type="button" class="list-group-item list-group-item-action disabled" v-if="!groups">
        <em>Empty</em>
      </button>
      <template v-bind:title="name" v-for="group in groups" v-else>
        <div class="list-group-item list-group-item-secondary" v-if="group.explicit">
          <div>
            <span class="verdict" v-bind:title="verdictTooltip(groupResult(group.name))" v-bind:class="verdictClass(groupResult(group.name))">{{ verdictLabel(groupResult(group.name)) }}</span>
            {{ group.name }}
          </div>
        </div>
        <button type="button" v-bind:class="{ 'in-group': group.explicit, active: currentCase == item.name }" class="list-group-item list-group-item-action d-flex justify-content-between align-items-center"  v-on:click="selectCase(item.name)" v-for="item in group.cases">
          <div>
            <span class="verdict" v-bind:title="verdictTooltip(caseResult(item.name))" v-bind:class="verdictClass(item.name)">{{ verdictLabel(caseResult(item.name)) }}</span>
            {{ item.name }}
          </div>
          <button v-if="item.name != 'sample'" type="button" class="close" aria-label="Close" v-on:click.prevent.stop="removeCase(item.name)">
              <span aria-hidden="true">&times;</span>
          </button>
        </button>
      </template>
    </div>
    <form v-on:submit.prevent="createCase()">
      <div class="input-group">
        <input type="text" class="form-control" v-model="newCaseName"></input>
        <div class="input-group-append">
          <button class="btn btn-secondary" type="button" v-bind:disabled="newCaseName.length == 0" v-on:click="createCase()">+</button>
        </div>
      </div>
    </form>
  </div>
</template>

<script>
import * as Util from './util';

export default {
  props: {
    store: Object,
    storeMapping: Object,
  },
  data: function() {
    return {
      newCaseName: '',
    };
  },
  computed: {
    summary: function() {
      if (!this.store.state.results || !this.store.state.results.verdict)
        return '…';
      return (this.store.state.results.verdict + ' ' +
              this.store.state.results.contest_score + '/' +
              this.store.state.results.max_score);
    },
    groups: function() {
      const flatCases = Util.vuexGet(this.store, this.storeMapping.cases);
      let resultMap = {};
      for (let caseName in flatCases) {
        if (!flatCases.hasOwnProperty(caseName)) continue;
        let tokens = caseName.split('.', 2);
        if (!resultMap.hasOwnProperty(tokens[0])) {
          resultMap[tokens[0]] = {
            explicit: tokens.length > 1,
            name: tokens[0],
            cases: [],
          };
        }
        resultMap[tokens[0]].cases.push({
          name: caseName,
          item: flatCases[caseName],
        });
      }
      let result = [];
      for (let groupName in resultMap) {
        if (!resultMap.hasOwnProperty(groupName)) continue;
        resultMap[groupName].cases.sort((a, b) => {
          if (a.name < b.name) return -1;
          if (a.name > b.name) return 1;
          return 0;
        });
        result.push(resultMap[groupName]);
      }
      result.sort((a, b) => {
        if (a.name < b.name) return -1;
        if (a.name > b.name) return 1;
        return 0;
      });
      return result;
    },
    currentCase: {
      get() {
        return Util.vuexGet(this.store, this.storeMapping.currentCase);
      },
      set(value) {
        Util.vuexSet(this.store, this.storeMapping.currentCase, value);
      },
    },
  },
  methods: {
    caseResult: function(caseName) {
      let flatCaseResults = this.store.getters.flatCaseResults;
      if (!flatCaseResults.hasOwnProperty(caseName)) return null;
      return flatCaseResults[caseName];
    },
    groupResult: function(groupName) {
      let results = this.store.state.results;
      if (!results || !results.groups) return null;
      for (let group of results.groups) {
        if (group.group == groupName) return group;
      }
      return null;
    },
    verdictLabel: function(result) {
      if (!result) return '…';
      if (typeof result.verdict === 'undefined') {
        if (result.contest_score == result.max_score) return '✓';
        return '✗';
      }
      switch(result.verdict) {
        case 'CE': return '…';
        case 'AC': return '✓';
        case 'PA': return '½';
        case 'WA': return '✗';
        case 'TLE': return '⌚';
      }
      return '  ☹';
    },
    verdictClass: function(result) {
      if (!result) return '';
      return result.verdict;
    },
    verdictTooltip: function(result) {
      if (!result) return '';
      let tooltip = '';
      if (typeof result.verdict !== 'undefined') {
        tooltip = result.verdict + ' ';
      }
      return (tooltip +
              result.contest_score + '/' +
              result.max_score);
    },
    selectCase: function(name) {
      this.currentCase = name;
    },
    createCase: function() {
      if (!this.newCaseName) return;
      this.store.commit('createCase', this.newCaseName);
      this.currentCase = this.newCaseName;
      this.newCaseName = '';
    },
    removeCase: function(name) {
      this.store.commit('removeCase', name);
    },
  },
};
</script>

<style scoped>
button.in-group {
  border-left-width: 6px;
  padding-left: 15px;
}
div.summary {
  text-align: center;
  padding: 0.25em;
}
span.verdict {
  display: inline-block;
  width: 1em;
}
div.filenames {
  overflow-y: auto;
  flex: 1;
}
a.list-group-item {
  white-space: nowrap;
  overflow: hidden;
  text-overflow: ellipsis;
}
</style>
