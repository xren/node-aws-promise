'use strict';
module.exports = function (grunt) {
    // load all grunt tasks
    require('matchdep').filterDev('grunt-*').forEach(grunt.loadNpmTasks);
    grunt.initConfig({
        jshint: {
            options: {
                jshintrc: '.jshintrc'
            },
            all: [
                'lib/*.js',
                'Gruntfile.js'
            ]
        },
        mochaTest: {
            files: [
                'test/*.js'
            ]
        },
        mochaTestConfig: {
            options: {
                reporter: 'spec'
            }
        },
        env: {
            development: {
                src: 'development.json'
            }
        }
    });

    // Default task.
    grunt.registerTask('test', ['env:development', 'jshint', 'mochaTest']);
    grunt.registerTask('travis', ['jshint', 'mochaTest']);
};
