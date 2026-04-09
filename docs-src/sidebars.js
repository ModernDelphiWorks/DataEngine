module.exports = {
  dataengineSidebar: [
    {
      type: 'category',
      label: 'DataEngine',
      link: { type: 'doc', id: 'dataengine/index' },
      items: [
        'dataengine/introduction',
        {
          type: 'category',
          label: 'Getting Started',
          items: ['dataengine/getting-started/quickstart'],
        },
        {
          type: 'category',
          label: 'Architecture',
          items: ['dataengine/architecture/overview', 'dataengine/architecture/runtime-flow'],
        },
        {
          type: 'category',
          label: 'Reference',
          items: ['dataengine/reference/api'],
        },
        {
          type: 'category',
          label: 'Guides',
          items: [
            'dataengine/guides/bulk-operations',
            'dataengine/guides/json-store',
            'dataengine/guides/async-execution',
            'dataengine/guides/connection-resiliency',
            'dataengine/guides/observability'
          ],

        },
        {
          type: 'category',
          label: 'Tests & Support',
          items: ['dataengine/tests/overview', 'dataengine/troubleshooting/common-errors'],
        },
      ],
    },
  ],
};