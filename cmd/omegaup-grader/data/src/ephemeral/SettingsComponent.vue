<template>
  <form>
    <div class="form-row">
      <div class="form-group col-md-4">
        <label for="inputTimeLimit">Time Limit</label>
        <input type="number" step="0.1" min="0.1" max="5.0" class="form-control" id="inputTimeLimit" v-model="timeLimit">
      </div>
      <div class="form-group col-md-4">
        <label for="inputOverallWallTimeLimit">Overall Wall Time Limit</label>
        <input type="number" step="0.1" min="0.1" max="5.0" class="form-control" id="inputOverallWallTimeLimit" v-model="overallWallTimeLimit">
      </div>
      <div class="form-group col-md-4">
        <label for="inputExtraWallTime">Extra Wall Time</label>
        <input type="number" step="0.1" min="0.0" max="5.0" class="form-control" id="inputExtraWallTime" v-model="extraWallTime">
      </div>
    </div>
    <div class="form-row">
      <div class="form-group col-md-6">
        <label for="inputMemoryLimit">Memory Limit</label>
        <input type="number" step="1048576" min="33554432" max="1073741824" class="form-control" id="inputMemoryLimit" v-model="memoryLimit">
      </div>
      <div class="form-group col-md-6">
        <label for="inputOutputLimit">Output Limit</label>
        <input type="number" step="1024" min="0" max="104857600" class="form-control" id="inputOutputLimit" v-model="outputLimit">
      </div>
    </div>
    <div class="form-row">
      <div class="form-group col-md-6">
        <label for="inputValidator">Validator</label>
        <select class="form-control" id="inputValidator" v-model="validator">
          <option value="custom">Custom</option>
          <option value="literal">Literal</option>
          <option value="token">Token</option>
          <option value="token-caseless">Token (Caseless)</option>
          <option value="token-numeric">Token (Numeric)</option>
        </select>
      </div>
      <div class="form-group col-md-6" v-if="validator == 'token-numeric'">
        <label for="inputTolerance">Tolerance</label>
        <input type="number" min="0" max="1" class="form-control" id="inputTolerance" v-model="tolerance">
      </div>
      <div class="form-group col-md-6" v-if="validator == 'custom'">
        <label for="inputValidatorLanguage">Language</label>
        <select class="form-control" id="inputValidatorLanguage" v-model="validatorLanguage">
          <option value="cpp">C++</option>
          <option value="py">Python</option>
        </select>
      </div>
    </div>
    <div class="form-row">
      <div class="form-group col-md-4">
        <label for="inputInteractive">Interactive</label>
        <select class="form-control" id="inputInteractive" v-model="interactive">
          <option v-bind:value="false">No</option>
          <option v-bind:value="true">Yes</option>
        </select>
      </div>
      <div class="form-group col-md-4" v-if="interactive">
        <label for="inputInteractiveModuleName">Module Name</label>
        <input class="form-control" id="inputInteractiveModuleName" v-model="interactiveModuleName">
      </div>
      <div class="form-group col-md-4" v-if="interactive">
        <label for="inputInteractiveLanguage">Language</label>
        <select class="form-control" id="inputInteractiveLanguage" v-model="interactiveLanguage">
          <option value="cpp11">C++</option>
          <option value="py">Python</option>
        </select>
      </div>
    </div>
  </form>
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
      title: 'settings',
    };
  },
  computed: {
    timeLimit: {
      get() {
        return this.store.state.request.input.limits.TimeLimit / 1000.0;
      },
      set(value) {
        this.store.commit('TimeLimit', value * 1000);
      },
    },
    overallWallTimeLimit: {
      get() {
        return this.store.state.request.input.limits.OverallWallTimeLimit / 1000.0;
      },
      set(value) {
        this.store.commit('OverallWallTimeLimit', value * 1000);
      },
    },
    extraWallTime: {
      get() {
        return this.store.state.request.input.limits.ExtraWallTime / 1000.0;
      },
      set(value) {
        this.store.commit('ExtraWallTime', value * 1000);
      },
    },
    memoryLimit: {
      get() {
        return this.store.state.request.input.limits.MemoryLimit;
      },
      set(value) {
        this.store.commit('MemoryLimit', Number.parseInt(value));
      },
    },
    outputLimit: {
      get() {
        return this.store.state.request.input.limits.OutputLimit;
      },
      set(value) {
        this.store.commit('OutputLimit', Number.parseInt(value));
      },
    },
    validator: {
      get() {
        return this.store.state.request.input.validator.name
      },
      set(value) {
        this.store.commit('Validator', value);
      },
    },
    tolerance: {
      get() {
        return this.store.state.request.input.validator.tolerance;
      },
      set(value) {
        this.store.commit('Tolerance', value);
      },
    },
    validatorLanguage: {
      get() {
        return this.store.state.request.input.validator.custom_validator.language;
      },
      set(value) {
        this.store.commit('ValidatorLanguage', value);
      },
    },
    interactive: {
      get() {
        return this.store.getters.isInteractive;
      },
      set(value) {
        this.store.commit('Interactive', value);
      },
    },
    interactiveLanguage: {
      get() {
        return this.store.state.request.input.interactive.language;
      },
      set(value) {
        this.store.commit('InteractiveLanguage', value);
      },
    },
    interactiveModuleName: {
      get() {
        return this.store.state.request.input.interactive.module_name;
      },
      set(value) {
        this.store.commit('InteractiveModuleName', value);
      },
    },
  },
};
</script>

<style scoped>
</style>
