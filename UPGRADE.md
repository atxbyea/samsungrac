# v9.0.0 Upgrade Guide

Version 9.0.0 introduces a new, simpler way to configure the integration through the Home Assistant user interface (using "Config Flow").

## New Configuration via UI (Recommended)

You can now add and configure the integration directly from the Home Assistant menu:

1.  Go to **Settings > Devices & Services**.
2.  Click **Add Integration** and search for "**Samsung AC**".
3.  Follow the steps in the configuration dialog.

**Key Feature: Auto-Discovery**
*   During setup, you can leave the **MAC Address** and **Token** fields blank.
*   The integration will attempt to obtain them automatically if you follow the on-screen instructions.

## YAML Configuration (Still Supported)

Your existing configuration in `configuration.yaml` will continue to work without changes.

**Recommendation:** We suggest you **do not delete your YAML configuration** for now. This will make it easier to downgrade to a previous version if you encounter any issues with v9.0.0.
