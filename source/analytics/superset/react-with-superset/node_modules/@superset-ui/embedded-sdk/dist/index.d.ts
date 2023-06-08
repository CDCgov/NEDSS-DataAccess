/**
 * The function to fetch a guest token from your Host App's backend server.
 * The Host App backend must supply an API endpoint
 * which returns a guest token with appropriate resource access.
 */
export declare type GuestTokenFetchFn = () => Promise<string>;
export declare type UiConfigType = {
    hideTitle?: boolean;
    hideTab?: boolean;
    hideChartControls?: boolean;
    filters?: {
        [key: string]: boolean | undefined;
        visible?: boolean;
        expanded?: boolean;
    };
};
export declare type EmbedDashboardParams = {
    /** The id provided by the embed configuration UI in Superset */
    id: string;
    /** The domain where Superset can be located, with protocol, such as: https://superset.example.com */
    supersetDomain: string;
    /** The html element within which to mount the iframe */
    mountPoint: HTMLElement;
    /** A function to fetch a guest token from the Host App's backend server */
    fetchGuestToken: GuestTokenFetchFn;
    /** The dashboard UI config: hideTitle, hideTab, hideChartControls, filters.visible, filters.expanded **/
    dashboardUiConfig?: UiConfigType;
    /** Are we in debug mode? */
    debug?: boolean;
};
export declare type Size = {
    width: number;
    height: number;
};
export declare type EmbeddedDashboard = {
    getScrollSize: () => Promise<Size>;
    unmount: () => void;
    getDashboardPermalink: (anchor: string) => Promise<string>;
    getActiveTabs: () => Promise<string[]>;
};
/**
 * Embeds a Superset dashboard into the page using an iframe.
 */
export declare function embedDashboard({ id, supersetDomain, mountPoint, fetchGuestToken, dashboardUiConfig, debug }: EmbedDashboardParams): Promise<EmbeddedDashboard>;
