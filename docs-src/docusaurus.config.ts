import type { Config } from '@docusaurus/types';

const config: Config = {
  title: 'DataEngine Docs',
  url: 'https://example.com',
  baseUrl: '/',
  favicon: 'img/favicon.ico',
  organizationName: 'ModernDelphiWorks',
  projectName: 'DataEngine',
  presets: [
    [
      'classic',
      {
        docs: {
          path: 'docs',
          routeBasePath: '/',
          sidebarPath: './sidebars.js',
        },
        blog: false,
        pages: false,
      },
    ],
  ],
  themeConfig: {
    navbar: {
      title: 'DataEngine',
      items: [
        {
          label: 'Projects',
          position: 'left',
          items: [{ to: '/dataengine/', label: 'DataEngine' }],
        },
      ],
    },
  },
};

export default config;
